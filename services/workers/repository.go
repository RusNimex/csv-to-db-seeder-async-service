package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	pivotBatchSize = 5000
)

// CompanyRepository реализует логику работы с БД, аналогичную PHP CompanyRepository
type CompanyRepository struct {
	db *sql.DB

	// Кэши справочников
	region      map[string]int
	district    map[string]int
	city        map[string]int
	category    map[string]int
	subcategory map[string]int
	company     map[string]int
	geoCache    map[string]int

	// Для массовой вставки связей
	companyGeos       map[int][]int
	companyCategories map[int]map[string][]int

	// Статистика
	companyCount int
	errors       []string

	mu sync.RWMutex
}

// NewCompanyRepository создает новый экземпляр репозитория
func NewCompanyRepository(db *sql.DB) *CompanyRepository {
	return &CompanyRepository{
		db:                db,
		region:            make(map[string]int),
		district:          make(map[string]int),
		city:              make(map[string]int),
		category:          make(map[string]int),
		subcategory:       make(map[string]int),
		company:           make(map[string]int),
		geoCache:          make(map[string]int),
		companyGeos:       make(map[int][]int),
		companyCategories: make(map[int]map[string][]int),
		errors:            make([]string, 0),
	}
}

// Insert вставляет данные в таблицу в определенном порядке
// 1. Предзагрузка всех справочников батчем (region, district, city, category, subcategory)
// 2. Отключаем проверку внешних ключей для ускорения вставки
// 3. Батч-вставка geo записей (зависит от region, district, city)
// 4. Батч-вставка компаний (независимая таблица)
// 5. Обработка связей
// 6. Массовая вставка связей (зависит от company, geo, category, subcategory)
// 7. Включаем обратно проверку внешних ключей
func (r *CompanyRepository) Insert(records []GisCompany) error {
	if len(records) == 0 {
		return nil
	}
	
	// Повторные попытки при deadlock (до 5 раз)
	maxRetries := 5
	var lastErr error
	
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			// Задержка перед повторной попыткой (экспоненциальная, минимум 500ms)
			delay := time.Duration(attempt*attempt) * 500 * time.Millisecond
			if delay < 500*time.Millisecond {
				delay = 500 * time.Millisecond
			}
			time.Sleep(delay)
			r.mu.Lock()
			r.addError(fmt.Sprintf("Повторная попытка импорта (попытка %d/%d)", attempt+1, maxRetries))
			r.mu.Unlock()
		}
		
		err := r.insertWithRetry(records)
		
		if err == nil {
			return nil
		}
		
		lastErr = err
		
		// Проверяем, является ли ошибка deadlock
		errStr := err.Error()
		if strings.Contains(errStr, "Deadlock") || strings.Contains(errStr, "deadlock") || 
		   strings.Contains(errStr, "Error 1213") {
			// Продолжаем повторные попытки
			continue
		}
		
		// Для других ошибок не повторяем
		return err
	}
	return fmt.Errorf("превышен лимит попыток при импорте: %w", lastErr)
}

// insertWithRetry выполняет одну попытку вставки данных
func (r *CompanyRepository) insertWithRetry(records []GisCompany) error {
	// Случайная задержка перед началом транзакции (100-600ms) для уменьшения вероятности deadlock
	// когда несколько воркеров одновременно обрабатывают похожие данные
	// Обязательная задержка минимум 100ms + случайная до 500ms
	randomDelay := 100*time.Millisecond + time.Duration(rand.Intn(500))*time.Millisecond
	time.Sleep(randomDelay)
	
	tx, err := r.db.Begin()
	if err != nil {
		return fmt.Errorf("ошибка начала транзакции: %w", err)
	}
	
	// Используем флаг для отслеживания статуса транзакции
	committed := false
	defer func() {
		if !committed {
			if tx != nil {
				if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
					// Игнорируем ошибку, если транзакция уже закоммичена или откачена
				}
			}
		}
	}()

	// Предзагрузка справочников в отдельных коротких транзакциях (ДО основной транзакции)
	// Это уменьшает время блокировки и вероятность deadlock
	// Справочники (region, district, city, category, subcategory) должны быть загружены
	// до вставки зависимых таблиц (geo, company, связи)
	if err := r.preloadDictionariesOutsideTx(records); err != nil {
		return fmt.Errorf("ошибка предзагрузки справочников: %w", err)
	}

	// Отключаем проверку внешних ключей для ускорения массовой вставки
	if _, err := tx.Exec("SET FOREIGN_KEY_CHECKS = 0"); err != nil {
		return fmt.Errorf("ошибка отключения FK: %w", err)
	}

	// Батч-вставка geo (зависит от region, district, city, которые уже предзагружены)
	if err := r.batchInsertGeo(tx, records); err != nil {
		return fmt.Errorf("ошибка батч-вставки geo: %w", err)
	}

	// Батч-вставка компаний
	if err := r.batchInsertCompanies(tx, records); err != nil {
		return fmt.Errorf("ошибка батч-вставки компаний: %w", err)
	}

	// Обработка связей
	for _, record := range records {
		geoID := r.getGeoID(record)
		if geoID == 0 {
			continue
		}

		categoryIDs, subcategoryIDs := r.getCategoryIDs(record)

		r.mu.RLock()
		companyID := r.company[record.Name]
		r.mu.RUnlock()

		if companyID == 0 {
			continue
		}

		r.collectCompanyGeos(companyID, geoID)
		r.collectCompanyCategories(companyID, categoryIDs, subcategoryIDs)
	}

	// Массовая вставка связей
	if err := r.insertCompanyGeos(tx); err != nil {
		return fmt.Errorf("ошибка вставки связей company_geo: %w", err)
	}

	if err := r.insertCompanyCategories(tx); err != nil {
		return fmt.Errorf("ошибка вставки связей company_categories: %w", err)
	}

	// Включаем обратно проверку внешних ключей
	if _, err := tx.Exec("SET FOREIGN_KEY_CHECKS = 1"); err != nil {
		// Игнорируем ошибку при восстановлении FK проверки
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("ошибка коммита транзакции: %w", err)
	}

	// Помечаем транзакцию как закоммиченную, чтобы defer не пытался её откатить
	committed = true
	return nil
}

// GetSummary возвращает статистику импорта
func (r *CompanyRepository) GetSummary() Summary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return Summary{
		Company:      r.companyCount,
		Category:     len(r.companyCategories),
		Subcategory:  len(r.subcategory),
		Region:       len(r.region),
		District:     len(r.district),
		City:         len(r.city),
		Errors:       r.errors,
	}
}

// preloadDictionaries предзагружает все справочники батчем
func (r *CompanyRepository) preloadDictionaries(tx *sql.Tx, records []GisCompany) error {
	uniqueValues := map[string]map[string]bool{
		"region":      make(map[string]bool),
		"district":    make(map[string]bool),
		"city":        make(map[string]bool),
		"category":    make(map[string]bool),
		"subcategory": make(map[string]bool),
	}

	// Собираем уникальные значения из всех записей
	for _, record := range records {
		if record.Region != "" {
			uniqueValues["region"][record.Region] = true
		}
		if record.District != "" {
			uniqueValues["district"][record.District] = true
		}
		if record.City != "" {
			uniqueValues["city"][record.City] = true
		}

		categories := r.extractCategories(record.Category)
		for _, cat := range categories {
			if cat != "" {
				uniqueValues["category"][cat] = true
			}
		}

		subcategories := r.extractCategories(record.Subcategory)
		for _, subcat := range subcategories {
			if subcat != "" {
				uniqueValues["subcategory"][subcat] = true
			}
		}
	}

	// Батч-вставка для каждого справочника
	tablesOrder := []string{"region", "district", "city", "category", "subcategory"}
	for _, table := range tablesOrder {
		values := uniqueValues[table]
		if len(values) > 0 {
			names := make([]string, 0, len(values))
			for name := range values {
				names = append(names, name)
			}
			if err := r.batchInsertDictionary(tx, table, names); err != nil {
				return fmt.Errorf("ошибка предзагрузки справочника %s: %w", table, err)
			}
		}
	}

	return nil
}

// preloadDictionariesOutsideTx предзагружает справочники в отдельных транзакциях
// Это уменьшает вероятность deadlock при одновременной обработке несколькими воркерами
func (r *CompanyRepository) preloadDictionariesOutsideTx(records []GisCompany) error {
	uniqueValues := map[string]map[string]bool{
		"region":      make(map[string]bool),
		"district":    make(map[string]bool),
		"city":        make(map[string]bool),
		"category":    make(map[string]bool),
		"subcategory": make(map[string]bool),
	}

	// Собираем уникальные значения из всех записей
	for _, record := range records {
		if record.Region != "" {
			uniqueValues["region"][record.Region] = true
		}
		if record.District != "" {
			uniqueValues["district"][record.District] = true
		}
		if record.City != "" {
			uniqueValues["city"][record.City] = true
		}

		categories := r.extractCategories(record.Category)
		for _, cat := range categories {
			if cat != "" {
				uniqueValues["category"][cat] = true
			}
		}

		subcategories := r.extractCategories(record.Subcategory)
		for _, subcat := range subcategories {
			if subcat != "" {
				uniqueValues["subcategory"][subcat] = true
			}
		}
	}

	// Батч-вставка для каждого справочника в ОТДЕЛЬНОЙ транзакции
	tablesOrder := []string{"region", "district", "city", "category", "subcategory"}
	for i, table := range tablesOrder {
		values := uniqueValues[table]
		if len(values) == 0 {
			continue
		}

		// Добавляем случайную задержку между справочниками
		// для уменьшения одновременных обращений к БД разных воркеров
		if i > 0 {
			// Задержка 50-150ms между справочниками
			interTableDelay := 50*time.Millisecond + time.Duration(rand.Intn(100))*time.Millisecond
			time.Sleep(interTableDelay)
		}

		names := make([]string, 0, len(values))
		for name := range values {
			names = append(names, name)
		}

		// Каждый справочник предзагружается в отдельной короткой транзакции
		maxRetries := 3
		var lastErr error
		
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				// Экспоненциальная задержка между попытками (минимум 500ms)
				// Увеличена для уменьшения конкуренции при большом количестве воркеров
				delay := time.Duration(attempt*attempt) * 500 * time.Millisecond
				if delay < 500*time.Millisecond {
					delay = 500 * time.Millisecond
				}
				time.Sleep(delay)
			}

			// Создаем отдельную транзакцию для каждого справочника
			tx, err := r.db.Begin()
			if err != nil {
				lastErr = fmt.Errorf("ошибка начала транзакции для справочника %s: %w", table, err)
				continue
			}

			err = r.batchInsertDictionary(tx, table, names)
			if err == nil {
				if err := tx.Commit(); err != nil {
					tx.Rollback()
					lastErr = fmt.Errorf("ошибка коммита транзакции для справочника %s: %w", table, err)
					continue
				}
				// Успешно
				break
			}

			// Откатываем транзакцию при ошибке
			tx.Rollback()
			lastErr = err

			// Проверяем, является ли ошибка deadlock
			errStr := err.Error()
			if strings.Contains(errStr, "Deadlock") || strings.Contains(errStr, "deadlock") || 
			   strings.Contains(errStr, "Error 1213") {
				if attempt < maxRetries-1 {
					continue // Повторяем попытку
				}
			} else {
				// Для других ошибок не повторяем
				return fmt.Errorf("ошибка предзагрузки справочника %s: %w", table, err)
			}
		}

		if lastErr != nil {
			return fmt.Errorf("ошибка предзагрузки справочника %s после %d попыток: %w", table, maxRetries, lastErr)
		}
	}

	return nil
}

// batchInsertDictionary батч-вставка справочника (только новые значения)
func (r *CompanyRepository) batchInsertDictionary(tx *sql.Tx, table string, names []string) error {
	if len(names) == 0 {
		return nil
	}

	// Фильтруем уже загруженные значения для вставки
	newNames := make([]string, 0)
	namesToLoad := make([]string, 0) // Все имена, которые нужно загрузить в кэш
	r.mu.RLock()
	cache := r.getCacheForTable(table)
	r.mu.RUnlock()

	for _, name := range names {
		if _, exists := cache[name]; !exists {
			// Если нет в кэше, нужно вставить и загрузить
			newNames = append(newNames, name)
			namesToLoad = append(namesToLoad, name)
		} else {
			// Если есть в кэше, но всё равно нужно убедиться, что оно загружено
			// (на случай, если кэш был очищен или это другой воркер)
			namesToLoad = append(namesToLoad, name)
		}
	}

	// Вставляем только новые значения батчем
	if len(newNames) > 0 {
		// Используем INSERT IGNORE для избежания ошибок при одновременной вставке
		placeholders := strings.Repeat("(?),", len(newNames))
		placeholders = placeholders[:len(placeholders)-1] // Убираем последнюю запятую
		query := fmt.Sprintf("INSERT IGNORE INTO csv.%s (name) VALUES %s", table, placeholders)

		args := make([]interface{}, len(newNames))
		for i, name := range newNames {
			args[i] = name
		}

		if _, err := tx.Exec(query, args...); err != nil {
			r.addError(fmt.Sprintf("ошибка при вставке %s: %v", table, err))
			return err
		}
	}

	// Загружаем ID для ВСЕХ используемых значений (не только новых)
	// Это гарантирует, что все категории/подкатегории из текущего батча будут в кэше
	return r.loadDictionaryFromDB(tx, table, namesToLoad)
}

// loadDictionaryFromDB загружает в кэш ID справочника из БД
// Разбивает большие списки на батчи для избежания превышения лимита параметров MySQL
func (r *CompanyRepository) loadDictionaryFromDB(tx *sql.Tx, table string, names []string) error {
	if len(names) == 0 {
		return nil
	}

	// MySQL имеет лимит на количество параметров в запросе (обычно 65535)
	// Разбиваем на батчи по 10000 элементов для безопасности
	const batchSize = 10000

	for i := 0; i < len(names); i += batchSize {
		end := i + batchSize
		if end > len(names) {
			end = len(names)
		}
		batch := names[i:end]

		placeholders := strings.Repeat("?,", len(batch))
		placeholders = placeholders[:len(placeholders)-1]
		query := fmt.Sprintf("SELECT id, name FROM csv.%s WHERE name IN (%s)", table, placeholders)

		args := make([]interface{}, len(batch))
		for j, name := range batch {
			args[j] = name
		}

		rows, err := tx.Query(query, args...)
		if err != nil {
			r.addError(fmt.Sprintf("ошибка при загрузке %s: %v", table, err))
			return err
		}

		r.mu.Lock()
		for rows.Next() {
			var id int
			var name string
			if err := rows.Scan(&id, &name); err != nil {
				r.mu.Unlock()
				rows.Close()
				return err
			}

			// Обновляем соответствующий кэш в зависимости от таблицы
			switch table {
			case "region":
				r.region[name] = id
			case "district":
				r.district[name] = id
			case "city":
				r.city[name] = id
			case "category":
				r.category[name] = id
			case "subcategory":
				r.subcategory[name] = id
			}
		}
		r.mu.Unlock()

		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
	}

	return nil
}

// getCacheForTable возвращает соответствующий кэш (только для чтения)
func (r *CompanyRepository) getCacheForTable(table string) map[string]int {
	switch table {
	case "region":
		return r.region
	case "district":
		return r.district
	case "city":
		return r.city
	case "category":
		return r.category
	case "subcategory":
		return r.subcategory
	default:
		return nil
	}
}

// batchInsertGeo батч-вставка geo записей
func (r *CompanyRepository) batchInsertGeo(tx *sql.Tx, records []GisCompany) error {
	geoData := make(map[string][3]*int)

	for _, record := range records {
		r.mu.RLock()
		regionID := r.getIDFromCache(r.region, record.Region)
		districtID := r.getIDFromCache(r.district, record.District)
		cityID := r.getIDFromCache(r.city, record.City)
		r.mu.RUnlock()

		if regionID == nil && districtID == nil && cityID == nil {
			continue
		}

		key := r.buildGeoKey(regionID, districtID, cityID)
		if _, exists := geoData[key]; !exists {
			geoData[key] = [3]*int{regionID, districtID, cityID}
		}
	}

	if len(geoData) == 0 {
		return nil
	}

	// Конвертируем map в slice для батч-обработки
	geoList := make([][3]*int, 0, len(geoData))
	for _, geo := range geoData {
		geoList = append(geoList, geo)
	}

	// Разбиваем на батчи (максимум 10000 записей на батч, чтобы не превысить лимит MySQL 65535 параметров)
	// 10000 * 3 = 30000 параметров - безопасный размер
	const geoBatchSize = 10000
	for i := 0; i < len(geoList); i += geoBatchSize {
		end := i + geoBatchSize
		if end > len(geoList) {
			end = len(geoList)
		}
		batch := geoList[i:end]

		// Вставляем батч geo записей
		placeholders := strings.Repeat("(?, ?, ?),", len(batch))
		placeholders = placeholders[:len(placeholders)-1]
		query := fmt.Sprintf("INSERT IGNORE INTO csv.geo (region_id, district_id, city_id) VALUES %s", placeholders)

		args := make([]interface{}, 0, len(batch)*3)
		for _, geo := range batch {
			args = append(args, geo[0], geo[1], geo[2])
		}

		if _, err := tx.Exec(query, args...); err != nil {
			r.addError(fmt.Sprintf("ошибка при вставке geo: %v", err))
			return err
		}
	}

	// Загружаем ID обратно в кэш батчами
	return r.loadGeoFromDB(tx, geoList)
}

// loadGeoFromDB загружает geo ID из БД в кэш батчами
// Использует временную таблицу для эффективного поиска вместо большого WHERE с OR
func (r *CompanyRepository) loadGeoFromDB(tx *sql.Tx, geoList [][3]*int) error {
	if len(geoList) == 0 {
		return nil
	}

	// Создаем временную таблицу для эффективного поиска
	tempTableName := fmt.Sprintf("temp_geo_%d", time.Now().UnixNano())
	createTempTable := fmt.Sprintf(`
		CREATE TEMPORARY TABLE %s (
			region_id INT NULL,
			district_id INT NULL,
			city_id INT NULL,
			INDEX idx_geo (region_id, district_id, city_id)
		) ENGINE=Memory
	`, tempTableName)
	
	if _, err := tx.Exec(createTempTable); err != nil {
		return fmt.Errorf("ошибка создания временной таблицы: %w", err)
	}

	// Разбиваем на батчи для вставки в временную таблицу
	const tempTableBatchSize = 5000
	for i := 0; i < len(geoList); i += tempTableBatchSize {
		end := i + tempTableBatchSize
		if end > len(geoList) {
			end = len(geoList)
		}
		batch := geoList[i:end]

		placeholders := strings.Repeat("(?, ?, ?),", len(batch))
		placeholders = placeholders[:len(placeholders)-1]
		insertQuery := fmt.Sprintf("INSERT INTO %s (region_id, district_id, city_id) VALUES %s",
			tempTableName, placeholders)

		args := make([]interface{}, 0, len(batch)*3)
		for _, geo := range batch {
			args = append(args, geo[0], geo[1], geo[2])
		}

		if _, err := tx.Exec(insertQuery, args...); err != nil {
			return fmt.Errorf("ошибка вставки во временную таблицу: %w", err)
		}
	}

	// Выполняем JOIN для получения ID
	query := fmt.Sprintf(`
		SELECT g.id, g.region_id, g.district_id, g.city_id 
		FROM csv.geo g
		INNER JOIN %s t ON (
			(g.region_id <=> t.region_id) AND 
			(g.district_id <=> t.district_id) AND 
			(g.city_id <=> t.city_id)
		)
	`, tempTableName)

	rows, err := tx.Query(query)
	if err != nil {
		return fmt.Errorf("ошибка при загрузке geo: %w", err)
	}
	defer rows.Close()

	r.mu.Lock()
	for rows.Next() {
		var id int
		var regionID, districtID, cityID sql.NullInt64
		if err := rows.Scan(&id, &regionID, &districtID, &cityID); err != nil {
			r.mu.Unlock()
			return err
		}

		key := r.buildGeoKey(
			r.nullIntToPtr(regionID),
			r.nullIntToPtr(districtID),
			r.nullIntToPtr(cityID),
		)
		r.geoCache[key] = id
	}
	
	if err := rows.Err(); err != nil {
		r.mu.Unlock()
		return err
	}
	r.mu.Unlock()

	// Временная таблица автоматически удалится при завершении транзакции
	return nil
}

// getGeoID получает geo ID из кэша
func (r *CompanyRepository) getGeoID(record GisCompany) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	regionID := r.getIDFromCache(r.region, record.Region)
	districtID := r.getIDFromCache(r.district, record.District)
	cityID := r.getIDFromCache(r.city, record.City)

	if regionID == nil && districtID == nil && cityID == nil {
		return 0
	}

	key := r.buildGeoKey(regionID, districtID, cityID)
	return r.geoCache[key]
}

// getCategoryIDs получает ID категорий и подкатегорий из кэша
func (r *CompanyRepository) getCategoryIDs(record GisCompany) ([]int, []int) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	categoryIDs := make([]int, 0)
	subcategoryIDs := make([]int, 0)

	categories := r.extractCategories(record.Category)
	for _, category := range categories {
		if category != "" {
			if id, exists := r.category[category]; exists {
				categoryIDs = append(categoryIDs, id)
			}
		}
	}

	subcategories := r.extractCategories(record.Subcategory)
	for _, subcategory := range subcategories {
		if subcategory != "" {
			if id, exists := r.subcategory[subcategory]; exists {
				subcategoryIDs = append(subcategoryIDs, id)
			}
		}
	}

	return categoryIDs, subcategoryIDs
}

// batchInsertCompanies батч-вставка компаний
func (r *CompanyRepository) batchInsertCompanies(tx *sql.Tx, records []GisCompany) error {
	uniqueCompanies := make(map[string]bool)

	r.mu.RLock()
	for _, record := range records {
		if record.Name != "" {
			if _, exists := r.company[record.Name]; !exists {
				uniqueCompanies[record.Name] = true
			}
		}
	}
	r.mu.RUnlock()

	if len(uniqueCompanies) == 0 {
		return nil
	}

	companyNames := make([]string, 0, len(uniqueCompanies))
	for name := range uniqueCompanies {
		companyNames = append(companyNames, name)
	}

	placeholders := strings.Repeat("(?),", len(companyNames))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf("INSERT IGNORE INTO csv.company (name) VALUES %s", placeholders)

	args := make([]interface{}, len(companyNames))
	for i, name := range companyNames {
		args[i] = name
	}

	if _, err := tx.Exec(query, args...); err != nil {
		r.addError(fmt.Sprintf("ошибка при вставке компаний: %v", err))
		return err
	}

	return r.loadCompaniesFromDB(tx, companyNames)
}

// loadCompaniesFromDB загружает ID компаний из БД
func (r *CompanyRepository) loadCompaniesFromDB(tx *sql.Tx, names []string) error {
	if len(names) == 0 {
		return nil
	}

	// Фильтруем уже загруженные
	newNames := make([]string, 0)
	r.mu.RLock()
	for _, name := range names {
		if name != "" {
			if _, exists := r.company[name]; !exists {
				newNames = append(newNames, name)
			}
		}
	}
	r.mu.RUnlock()

	if len(newNames) == 0 {
		return nil
	}

	placeholders := strings.Repeat("?,", len(newNames))
	placeholders = placeholders[:len(placeholders)-1]
	query := fmt.Sprintf("SELECT id, name FROM csv.company WHERE name IN (%s)", placeholders)

	args := make([]interface{}, len(newNames))
	for i, name := range newNames {
		args[i] = name
	}

	rows, err := tx.Query(query, args...)
	if err != nil {
		r.addError(fmt.Sprintf("ошибка при загрузке компаний: %v", err))
		return err
	}
	defer rows.Close()

	r.mu.Lock()
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			r.mu.Unlock()
			return err
		}
		if _, exists := r.company[name]; !exists {
			r.company[name] = id
			r.companyCount++
		}
	}
	r.mu.Unlock()

	return nil
}

// extractCategories выделяет категории/подкатегории из строки, разделенной запятой
func (r *CompanyRepository) extractCategories(commaValues string) []string {
	if commaValues == "" {
		return []string{}
	}

	values := strings.Split(commaValues, ",")
	sanitized := make([]string, 0, len(values))
	for _, v := range values {
		sanitized = append(sanitized, strings.TrimSpace(v))
	}

	if len(sanitized) == 0 {
		return []string{}
	}

	// Если строка длинная или много элементов - удаляем последний
	strLength := utf8.RuneCountInString(commaValues)
	elementCount := len(sanitized)

	if elementCount > 1 {
		lastValue := sanitized[elementCount-1]
		lastLength := utf8.RuneCountInString(lastValue)
		shouldRemove := false

		// Удаляем последний элемент если:
		// 1. Строка очень длинная (>= 540 символов)
		// 2. ИЛИ последний элемент подозрительно короткий (< 4 символов)
		// 3. ИЛИ последний элемент заканчивается на "/" или "-" или ","
		if strLength >= 540 {
			shouldRemove = true
		} else if lastLength < 4 && elementCount > 2 {
			shouldRemove = true
		} else {
			lastValueTrimmed := strings.TrimSpace(lastValue)
			if len(lastValueTrimmed) > 0 {
				lastChar := string([]rune(lastValueTrimmed)[len([]rune(lastValueTrimmed))-1])
				if lastChar == "/" || lastChar == "-" || lastChar == "," {
					shouldRemove = true
				}
			}
		}

		if shouldRemove {
			sanitized = sanitized[:len(sanitized)-1]
		}
	}

	return sanitized
}

// collectCompanyGeos привязывает компанию к гео
func (r *CompanyRepository) collectCompanyGeos(companyID int, geoID int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.companyGeos[companyID] == nil {
		r.companyGeos[companyID] = make([]int, 0)
	}

	// Проверяем, нет ли уже такой связи
	for _, existingGeoID := range r.companyGeos[companyID] {
		if existingGeoID == geoID {
			return
		}
	}

	r.companyGeos[companyID] = append(r.companyGeos[companyID], geoID)
}

// collectCompanyCategories привязывает компанию к категориям и подкатегориям
func (r *CompanyRepository) collectCompanyCategories(companyID int, categoryIDs []int, subcategoryIDs []int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.companyCategories[companyID] == nil {
		r.companyCategories[companyID] = make(map[string][]int)
	}

	// Добавляем категории
	if r.companyCategories[companyID]["category"] == nil {
		r.companyCategories[companyID]["category"] = make([]int, 0)
	}
	r.companyCategories[companyID]["category"] = r.uniqueInts(
		append(r.companyCategories[companyID]["category"], categoryIDs...),
	)

	// Добавляем подкатегории
	if r.companyCategories[companyID]["subcategory"] == nil {
		r.companyCategories[companyID]["subcategory"] = make([]int, 0)
	}
	r.companyCategories[companyID]["subcategory"] = r.uniqueInts(
		append(r.companyCategories[companyID]["subcategory"], subcategoryIDs...),
	)
}

// insertCompanyGeos вставляет батчами привязку компаний к гео
func (r *CompanyRepository) insertCompanyGeos(tx *sql.Tx) error {
	r.mu.RLock()
	if len(r.companyGeos) == 0 {
		r.mu.RUnlock()
		return nil
	}

	// Собираем все связи в плоский массив
	allLinks := make([][2]int, 0)
	for companyID, geoIDs := range r.companyGeos {
		for _, geoID := range geoIDs {
			allLinks = append(allLinks, [2]int{companyID, geoID})
		}
	}
	r.mu.RUnlock()

	if len(allLinks) == 0 {
		return nil
	}

	// Разбиваем на батчи
	for i := 0; i < len(allLinks); i += pivotBatchSize {
		end := i + pivotBatchSize
		if end > len(allLinks) {
			end = len(allLinks)
		}
		batch := allLinks[i:end]

		placeholders := strings.Repeat("(?, ?),", len(batch))
		placeholders = placeholders[:len(placeholders)-1]
		query := fmt.Sprintf("INSERT IGNORE INTO csv.company_geo (company_id, geo_id) VALUES %s", placeholders)

		args := make([]interface{}, 0, len(batch)*2)
		for _, link := range batch {
			args = append(args, link[0], link[1])
		}

		if _, err := tx.Exec(query, args...); err != nil {
			r.addError(fmt.Sprintf("ошибка при вставке связей company_geo: %v", err))
			return err
		}
	}

	return nil
}

// insertCompanyCategories вставляет батчами привязку компаний к категориям и подкатегориям
func (r *CompanyRepository) insertCompanyCategories(tx *sql.Tx) error {
	r.mu.RLock()
	if len(r.companyCategories) == 0 {
		r.mu.RUnlock()
		return nil
	}
	r.mu.RUnlock()

	fields := []string{"category", "subcategory"}

	for _, fieldType := range fields {
		r.mu.RLock()
		allLinks := make([][2]int, 0)
		for companyID, types := range r.companyCategories {
			if types[fieldType] != nil {
				for _, valueID := range types[fieldType] {
					allLinks = append(allLinks, [2]int{companyID, valueID})
				}
			}
		}
		r.mu.RUnlock()

		if len(allLinks) == 0 {
			continue
		}

		// Разбиваем на батчи
		for i := 0; i < len(allLinks); i += pivotBatchSize {
			end := i + pivotBatchSize
			if end > len(allLinks) {
				end = len(allLinks)
			}
			batch := allLinks[i:end]

			placeholders := strings.Repeat("(?, ?),", len(batch))
			placeholders = placeholders[:len(placeholders)-1]
			query := fmt.Sprintf("INSERT IGNORE INTO csv.company_%s (company_id, %s_id) VALUES %s",
				fieldType, fieldType, placeholders)

			args := make([]interface{}, 0, len(batch)*2)
			for _, link := range batch {
				args = append(args, link[0], link[1])
			}

			if _, err := tx.Exec(query, args...); err != nil {
				r.addError(fmt.Sprintf("ошибка при вставке связей company_%s: %v", fieldType, err))
				return err
			}
		}
	}

	return nil
}

// Вспомогательные методы

func (r *CompanyRepository) getIDFromCache(cache map[string]int, key string) *int {
	if key == "" {
		return nil
	}
	if id, exists := cache[key]; exists {
		return &id
	}
	return nil
}

func (r *CompanyRepository) buildGeoKey(regionID, districtID, cityID *int) string {
	regionStr := ""
	if regionID != nil {
		regionStr = fmt.Sprintf("%d", *regionID)
	}
	districtStr := ""
	if districtID != nil {
		districtStr = fmt.Sprintf("%d", *districtID)
	}
	cityStr := ""
	if cityID != nil {
		cityStr = fmt.Sprintf("%d", *cityID)
	}
	return fmt.Sprintf("%s:%s:%s", regionStr, districtStr, cityStr)
}

func (r *CompanyRepository) nullIntToPtr(n sql.NullInt64) *int {
	if n.Valid {
		val := int(n.Int64)
		return &val
	}
	return nil
}

func (r *CompanyRepository) uniqueInts(ints []int) []int {
	seen := make(map[int]bool)
	result := make([]int, 0, len(ints))
	for _, v := range ints {
		if !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	return result
}

func (r *CompanyRepository) addError(err string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errors = append(r.errors, err)
}

