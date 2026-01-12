package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
)

// CSVParser парсит CSV файлы
type CSVParser struct {
	delimiter rune
}

// NewCSVParser создает новый парсер CSV
func NewCSVParser() *CSVParser {
	return &CSVParser{
		delimiter: ';',
	}
}

// ParseFile парсит CSV файл и возвращает массив записей
func (p *CSVParser) ParseFile(filePath string) ([]GisCompany, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("ошибка открытия файла: %w", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = p.delimiter
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true

	// Читаем заголовки
	headers, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("ошибка чтения заголовков: %w", err)
	}

	// Создаем мапу для быстрого поиска индексов колонок
	headerMap := make(map[string]int)
	for i, header := range headers {
		headerMap[strings.TrimSpace(header)] = i
	}

	var records []GisCompany

	// Читаем данные
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("ошибка чтения строки: %w", err)
		}

		// Создаем мапу для текущей строки
		rowMap := make(map[string]string)
		for i, header := range headers {
			if i < len(row) {
				rowMap[strings.TrimSpace(header)] = strings.TrimSpace(row[i])
			}
		}

		// Создаем GisCompany из строки CSV
		company := GisCompany{
			Name:        p.getField(rowMap, headerMap, "Название"),
			Region:      p.getField(rowMap, headerMap, "Регион"),
			District:    p.getField(rowMap, headerMap, "Район"),
			City:        p.getField(rowMap, headerMap, "Город"),
			Email:       p.getField(rowMap, headerMap, "Email"),
			Phone:       p.getField(rowMap, headerMap, "Телефон"),
			Category:    p.getField(rowMap, headerMap, "Рубрика"),
			Subcategory: p.getField(rowMap, headerMap, "Подрубрика"),
		}

		records = append(records, company)
	}

	return records, nil
}

// getField получает значение поля из строки CSV
func (p *CSVParser) getField(rowMap map[string]string, headerMap map[string]int, fieldName string) string {
	if val, ok := rowMap[fieldName]; ok {
		return val
	}
	return ""
}

