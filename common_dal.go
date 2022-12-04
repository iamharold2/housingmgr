package mssqldal

import (
	"database/sql"
	"fmt"
	"strings"

	"time"

	_ "github.com/denisenkom/go-mssqldb"
)

const SQLTYPE = "mssql"

var ConnString string = ""

func Get_pagination(sqlCount int, rowsCount int, page int, pageSize int) *PaginationModel {
	Pagination := PaginationModel{}
	Pagination.Page = page
	Pagination.PageSize = pageSize
	Pagination.Count = sqlCount
	Pagination.Start = (page*pageSize - pageSize) + 1
	Pagination.End = Pagination.Start + (rowsCount - 1)
	Pagination.PageCount = 1
	if Pagination.Count > pageSize {
		if Pagination.Count%pageSize != 0 {
			Pagination.PageCount = Pagination.Count/pageSize + 1
		} else {
			Pagination.PageCount = Pagination.Count / pageSize
		}

	}
	return &Pagination

}
func GetIterationCount(r *sql.Rows) int {
	count := 0
	for r.Next() {
		count++
	}
	return count
}

func GetCount(sqlstring string) (int, error) {
	count := 0
	conn, err := sql.Open(SQLTYPE, ConnString)
	if err != nil {
		return 0, nil
	}
	defer conn.Close()

	stmt, err := conn.Prepare(sqlstring)
	if err != nil {
		return 0, nil
	}
	rows, err := stmt.Query()
	if err != nil {
		return 0, nil
	}
	defer stmt.Close()
	for rows.Next() {
		rows.Scan(&count)
	}
	defer rows.Close()

	return count, nil
}
func GetPageReturnMaps(sqlstr string, pageSize int, page int, sortStr string) ([]map[string]interface{}, error) {
	start := (page-1)*pageSize + 1
	end := page * pageSize
	sqlText := "select * from (select ROW_NUMBER() over(order by %s ) as rowNumber,DENSE_RANK() over(order by %s) as stu_rank, * from ( %s ) c ) as temp where rowNumber between %d and %d;"
	sqlText = fmt.Sprintf(sqlText, sortStr, sortStr, sqlstr, start, end)

	conn, err := sql.Open(SQLTYPE, ConnString)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(sqlText)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	rows, _ := stmt.Query()
	defer rows.Close()
	cols, _ := rows.Columns()
	collen := len(cols)
	var result []map[string]interface{}
	var col = make(map[string]interface{})
	var ies = make([]interface{}, collen)
	for i := 0; i < collen; i++ {
		var ie interface{}
		ies[i] = &ie
	}

	for rows.Next() {
		err := rows.Scan(ies...)
		if err != nil {
			return nil, err
		}
		col = make(map[string]interface{})
		for i := 0; i < collen; i++ {
			col[cols[i]] = *ies[i].(*interface{})
		}

		result = append(result, col)
	}

	return result, nil
}
func GetPageReturnRows(sqlstr string, pageSize int, page int, sortStr string) (*sql.Rows, error) {
	start := (page-1)*pageSize + 1
	end := page * pageSize
	sqlText := "select * from (select ROW_NUMBER() over(order by %s ) as rowNumber,DENSE_RANK() over(order by %s) as stu_rank, * from ( %s ) c ) as temp where rowNumber between %d and %d;"
	sqlText = fmt.Sprintf(sqlText, sortStr, sortStr, sqlstr, start, end)

	conn, err := sql.Open("mssql", ConnString)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(sqlText)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.Query()
}

func GetListReturnRows(sqlstring string) (*sql.Rows, error) {
	conn, err := sql.Open("mssql", ConnString)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(sqlstring)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()

	return stmt.Query()

}
func ExistRow(rows []map[string]interface{}, key string, val string) (bool, error) {
	result := false
	for _, v := range rows {
		if v[key].(time.Time).Format("2006-01-02") == val {
			result = true
			break
		}

	}
	return result, nil
}
func GetListReturnMaps(sqlstring string) ([]map[string]interface{}, error) {
	conn, err := sql.Open("mssql", ConnString)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(sqlstring)
	if err != nil {
		return nil, err
	}
	defer stmt.Close()
	rows, _ := stmt.Query()

	cols, _ := rows.Columns()
	collen := len(cols)
	var result []map[string]interface{}
	var col = make(map[string]interface{})
	var ies = make([]interface{}, collen)
	for i := 0; i < collen; i++ {
		var ie interface{}
		ies[i] = &ie
	}

	for rows.Next() {
		err := rows.Scan(ies...)
		if err != nil {
			return nil, err
		}
		col = make(map[string]interface{})
		for i := 0; i < collen; i++ {
			col[cols[i]] = *ies[i].(*interface{})
		}

		result = append(result, col)
	}
	return result, nil

}
func ExecuteNonQuery(sqlstring string) error {

	conn, err := sql.Open("mssql", ConnString)
	if err != nil {
		return err
	}
	defer conn.Close()

	stmt, err := conn.Prepare(sqlstring)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		return err
	}
	return nil
}
func Get_order_max(data_sql string, field string) (int, error) {
	if len(field) == 0 {
		field = "sequence_"
	}
	result := 0
	var sql strings.Builder
	sql.WriteString(fmt.Sprintf("select top 1 * from ( %s ) t order by %s desc", data_sql, field))
	dtMap, err := GetListReturnMaps(sql.String())
	if err != nil {

		return result, err
	}
	if len(dtMap) > 0 {
		result = dtMap[0][field].(int)
	}
	return result, nil
}
func Insert_getid(sqlstring string) (int, error) {
	result := 0
	var sqlsb strings.Builder
	sqlsb.WriteString(sqlstring)
	sqlsb.WriteString(";SELECT @@Identity id;")
	conn, err := sql.Open("mssql", ConnString)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	stmt, err := conn.Prepare(sqlstring)
	if err != nil {
		return 0, err
	}
	defer stmt.Close()
	err = stmt.QueryRow("id").Scan(&result)
	if err != nil {
		return 0, err
	}

	return result, nil
}
