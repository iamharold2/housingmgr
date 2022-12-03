package mssqldal

type PaginationModel struct {
	Page      int `json:"page"`
	PageCount int `json:"pageCount"`
	Count     int `json:"count"`
	PageSize  int `json:"pageSize"`
	Start     int `json:"start"`
	End       int `json:"end"`
}
