--
-- Подбираем по ID компании похожие (конкурентов) в том же городе, что и целевая компания.
-- План действий:
--  1. Берем целевую компанию
--  2. Целевые категории/подкатегории и города целевой компании
--  3. Найдем компании в том же городе (можно и без него)
--  4. Найдем похожие по категории и подкатегории - через фильтр JOIN cat_target/subcat_target
--  5. Соберем список конкурентов - в том же городе и имеют наибольшее кол-во совпадений по категории/подкатегории
--  6. Выведем результат
--
WITH 
-- Целевая компания
company_target
AS
(
  SELECT
    co.id,
    co.name,
    (SELECT COUNT(cgeo.company_id) FROM company_geo cgeo WHERE cgeo.company_id = co.id) AS offices
  FROM company co
  WHERE co.id = 612225
),

-- Целевые категории
cat_target
AS
(
  SELECT
    c.name,
    cc.category_id,
    cc.company_id
  FROM company_category cc
  JOIN category c ON cc.category_id = c.id
  JOIN company_target ct ON cc.company_id = ct.id
),

-- Целевые подкатегории
subcat_target
AS
(
  SELECT
    sc.subcategory_id,
    sc.company_id
  FROM company_subcategory sc
  WHERE sc.company_id = (SELECT id FROM company_target)
),

-- Целевые города/регионы
geo_target
AS
(
  SELECT
    g.city_id
  FROM company_geo cg
  JOIN company_target ct ON cg.company_id = ct.id
  JOIN geo g ON cg.geo_id = g.id
),

-- Компании в том же городе
company_by_geo
AS 
(
  SELECT 
    gt.city_id,
    cg.company_id
  FROM company_geo cg 
  JOIN geo g ON g.id = cg.geo_id
  JOIN geo_target gt ON g.city_id = gt.city_id
),

-- Компании в той же категории. Это уже основные конкуренты. По ним собираем список конкурентов.
-- По этиму массиву подбор должне быть уже быстрым.
company_by_category
AS
(
  SELECT
    cc.company_id,
    COUNT(DISTINCT(cc.category_id)) as matching_category
  FROM company_category cc
  JOIN cat_target ct ON cc.category_id = ct.category_id
  GROUP BY cc.company_id
  ORDER BY matching_category DESC
  LIMIT 100000
),

-- Компании в той же подкатегории 
company_by_subcat 
AS 
(
  SELECT 
    s.company_id,
    COUNT(DISTINCT(s.subcategory_id)) AS matching_subcategory
  FROM company_subcategory s
  JOIN subcat_target st ON st.subcategory_id = s.subcategory_id
  GROUP BY s.company_id
),

-- Компании с совпадающими категориями и подкатегориями
-- В том же городе что и целевая
competitors 
AS 
(
  SELECT 
    c.company_id,
    c.matching_category AS categories,
    COALESCE(sub.matching_subcategory, 0) AS subcategories,
    cbg.city_id
  FROM company_by_category c
  LEFT JOIN company_by_subcat sub ON sub.company_id = c.company_id
  JOIN company_by_geo cbg ON c.company_id = cbg.company_id
)

-- Результат:
-- Компании той же категории/подкатегории
-- В той же локации
-- todo Кол-во офисов не должно превышать +/-2шт
SELECT
  co.id,
  co.name AS company,
  cocat.categories,
  cocat.subcategories,
  GROUP_CONCAT(DISTINCT ci.name) AS cities
FROM competitors cocat
JOIN company co ON co.id = cocat.company_id
LEFT JOIN city ci ON ci.id = cocat.city_id
WHERE co.id != (SELECT id FROM company_target)
GROUP BY co.id,
         co.name
ORDER BY cocat.categories DESC, cocat.subcategories DESC
LIMIT 100;

