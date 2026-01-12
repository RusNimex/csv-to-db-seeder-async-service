package main

// GisCompany представляет модель компании из CSV
type GisCompany struct {
	Name        string
	Region      string
	District    string
	City        string
	Email       string
	Phone       string
	Category    string
	Subcategory string
}

// ImportTask представляет задачу на импорт данных из API
type ImportTask struct {
	FilePath string `json:"file_path"`
	FileName string `json:"file_name"`
	FileSize int    `json:"file_size"`
	Priority string `json:"priority"`
	CreatedAt string `json:"created_at"`
}

// Summary представляет статистику импорта
type Summary struct {
	Company      int      `json:"company"`
	Category     int      `json:"category"`
	Subcategory  int      `json:"subcategory"`
	Region       int      `json:"region"`
	District     int      `json:"district"`
	City         int      `json:"city"`
	Errors       []string `json:"errors"`
}

