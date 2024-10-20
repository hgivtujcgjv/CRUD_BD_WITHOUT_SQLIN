package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type TableInfo struct {
	Name   string
	Id     string
	Fields []FieldInfo
}

type FieldInfo struct {
	Name     string
	Type     string
	Required bool
	IsKey    bool
}

type TablesContext struct {
	Tables     map[string]TableInfo
	TableNames []string
}

func (table *TableInfo) prepareUpdateParameters(params map[string]interface{}) []interface{} {
	result := make([]interface{}, 0)
	for _, v := range params {
		result = append(result, v)
	}
	return result
}

func (table *TableInfo) prepareDeleteQuery() string {
	return fmt.Sprintf("delete from %s where %s = ?", table.Name, table.Id)
}
func (table *TableInfo) prepareRow() []interface{} {
	row := make([]interface{}, len(table.Fields))
	for i, field := range table.Fields {
		switch field.Type {
		case "varchar", "text":
			row[i] = new(sql.NullString)
		case "int":
			row[i] = new(sql.NullInt64)
		}
	}
	return row
}

func (tablesCtxt *TablesContext) containsTable(table string) bool {
	_, ok := tablesCtxt.Tables[table]
	return ok
}
func (table *TableInfo) make_insert_query() string {
	result := make([]string, len(table.Fields))
	placeholders := make([]string, len(table.Fields))
	for i, field := range table.Fields {
		result[i] = field.Name
		placeholders[i] = "?"
	}
	return fmt.Sprintf("insert into %s (%s) values (%s)", table.Name, strings.Join(result, ", "), strings.Join(placeholders, ", "))
}
func (field *FieldInfo) getDefaultValue() interface{} {
	switch field.Type {
	case "varchar":
		return ""
	case "text":
		return ""
	case "int":
		return 0
	}
	return nil
}
func (table *TableInfo) prepareUpdateQuery(params map[string]interface{}) string {
	values := make([]string, 0)
	for k := range params {
		values = append(values, fmt.Sprintf("%s = ?", k))
	}
	return fmt.Sprintf("update %s set %s where %s = ?", table.Name, strings.Join(values, ","), table.Id)
}

func (table *TableInfo) transformRow(row []interface{}) map[string]interface{} {
	item := make(map[string]interface{}, len(row))
	for i, v := range row {
		switch table.Fields[i].Type {
		case "varchar", "text":
			if value, ok := v.(*sql.NullString); ok && value.Valid {
				item[table.Fields[i].Name] = value.String
			} else {
				item[table.Fields[i].Name] = nil
			}
		case "int":
			if value, ok := v.(*sql.NullInt64); ok && value.Valid {
				item[table.Fields[i].Name] = value.Int64
			} else {
				item[table.Fields[i].Name] = nil
			}
		}
	}
	return item
}

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	tablesContext, err := initContext(db)
	if err != nil {
		return nil, err
	}

	serverMux := http.NewServeMux()
	serverMux.HandleFunc("/", func(writer http.ResponseWriter, request *http.Request) {
		switch request.Method {
		case http.MethodGet:
			path := request.URL.Path
			if path == "/" {
				result, err := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"tables": tablesContext.TableNames}})
				if err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					return
				}
				writer.Write(result)
				return
			}
			fragments := strings.Split(path, "/")

			switch len(fragments) {
			case 2:
				tableName := fragments[1]
				if !tablesContext.containsTable(tableName) {
					result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
					writer.WriteHeader(http.StatusNotFound)
					writer.Write(result)
					return
				}

				limit := 5
				offset := 0

				if request.URL.Query().Get("limit") != "" {
					limit, err = strconv.Atoi(request.URL.Query().Get("limit"))
					if err != nil {
						limit = 5
					}
				}
				if request.URL.Query().Get("offset") != "" {
					offset, err = strconv.Atoi(request.URL.Query().Get("offset"))
					if err != nil {
						offset = 0
					}
				}

				rows, err := getRows(db, tablesContext.Tables[tableName], limit, offset)
				if err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					return
				}

				result, err := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"records": rows}})
				if err != nil {
					writer.WriteHeader(http.StatusInternalServerError)
					println(err.Error())
					return
				}
				writer.Write(result)
			case 3:
				tableName := fragments[1]
				if !tablesContext.containsTable(tableName) {
					result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
					writer.WriteHeader(http.StatusNotFound)
					writer.Write(result)
					return
				}
				tableId := fragments[2]
				rows, err := getRow(db, tablesContext.Tables[tableName], tableId)
				if err != nil {
					writer.WriteHeader(http.StatusNotFound)
					result, _ := json.Marshal(map[string]string{"error": "record not found"})
					writer.Write(result)
					return
				}
				result, _ := json.Marshal(
					map[string]interface{}{"response": map[string]interface{}{"record": rows}})
				writer.Write(result)

			}
		case http.MethodPut:
			path := request.URL.Path
			parts := strings.Split(path, "/")
			tableName := parts[1]
			if !tablesContext.containsTable(tableName) {
				result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
				writer.WriteHeader(http.StatusNotFound)
				writer.Write(result)
				return
			}
			table := tablesContext.Tables[tableName]
			decoder := json.NewDecoder(request.Body)
			req_param := make(map[string]interface{}, len(table.Fields))
			decoder.Decode(&req_param)
			result, err := insertRow(db, tablesContext.Tables[tableName], req_param)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				println(err.Error())
				return
			}
			resultBytes, _ := json.Marshal(map[string]interface{}{"response": map[string]interface{}{table.Id: result}})
			writer.Write(resultBytes)
		case http.MethodPost:
			path := request.URL.Path
			parts := strings.Split(path, "/")
			tableName := parts[1]
			if !tablesContext.containsTable(tableName) {
				result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
				writer.WriteHeader(http.StatusNotFound)
				writer.Write(result)
				return
			}
			table := tablesContext.Tables[tableName]
			decoder := json.NewDecoder(request.Body)
			req_param := make(map[string]interface{}, len(table.Fields))
			decoder.Decode(&req_param)
			valid_err := table.make_post_(req_param, true)
			if valid_err != nil {
				result, _ := json.Marshal(map[string]interface{}{"error": valid_err.Error()})
				writer.WriteHeader(http.StatusBadRequest)
				writer.Write(result)
				return
			}
			id := parts[2]
			result, err := updateRow(db, table, id, req_param)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				println(err.Error())
				return
			}
			resultBytes, _ := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"updated": result}})
			writer.Write(resultBytes)
		case http.MethodDelete:
			path := request.URL.Path
			parts := strings.Split(path, "/")
			tableName := parts[1]
			tableId := parts[2]
			if !tablesContext.containsTable(tableName) {
				result, _ := json.Marshal(map[string]interface{}{"error": "unknown table"})
				writer.WriteHeader(http.StatusNotFound)
				writer.Write(result)
				return
			}
			table := tablesContext.Tables[tableName]
			result, err := deleteRow(db, table, tableId)
			if err != nil {
				writer.WriteHeader(http.StatusInternalServerError)
				println(err.Error())
				return
			}
			resultBytes, _ := json.Marshal(map[string]interface{}{"response": map[string]interface{}{"deleted": result}})
			writer.Write(resultBytes)
		}
	})

	return serverMux, nil
}

func deleteRow(db *sql.DB, table TableInfo, id interface{}) (int64, error) {
	query := table.prepareDeleteQuery()
	res, err := db.Exec(query, id)
	if err != nil {
		return 0, err
	} else {
		answer, _ := res.RowsAffected()
		return answer, nil
	}
}
func (field *FieldInfo) validateField(value interface{}) error {
	if value == nil && field.Required {
		return fmt.Errorf("field %s have invalid type", field.Name)
	}
	switch value.(type) {
	case float64:
		if field.Type != "int" {
			return fmt.Errorf("field %s have invalid type", field.Name)
		}
	case string:
		if field.Type != "varchar" && field.Type != "text" {
			return fmt.Errorf("field %s have invalid type", field.Name)
		}
	}
	return nil
}
func (table TableInfo) make_post_(params map[string]interface{}, valid_field bool) error {
	for _, field := range table.Fields {
		if value, ok := params[field.Name]; ok {
			if valid_field && field.IsKey {
				return fmt.Errorf("field %s have invalid type", field.Name)
			}
			err := field.validateField(value)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (table *TableInfo) prepare_insert_parametrs(params map[string]interface{}) []interface{} {
	fmt.Printf("preparing parameters %v\n", params)
	result := make([]interface{}, len(table.Fields))
	for i, field := range table.Fields {
		if table.Id == field.Name {
			continue
		}
		if params[field.Name] == nil {
			if !field.Required {
				result[i] = nil
			} else {
				result[i] = field.getDefaultValue()
			}
		} else {
			result[i] = params[field.Name]
		}
	}
	return result
}
func updateRow(db *sql.DB, table TableInfo, id interface{}, params map[string]interface{}) (int64, error) {
	query := table.prepareUpdateQuery(params)
	println(query)
	queryParams := table.prepareUpdateParameters(params)
	queryParams = append(queryParams, id)
	fmt.Printf("parameters %v\n", queryParams)
	res, err := db.Exec(query, queryParams...)
	if err != nil {
		return 0, err
	} else {
		result, _ := res.RowsAffected()
		return result, nil
	}
}

func insertRow(db *sql.DB, table TableInfo, request_params map[string]interface{}) (int64, error) {
	query := table.make_insert_query()
	println(query)
	queryParams := table.prepare_insert_parametrs(request_params)
	res, err := db.Exec(query, queryParams...)
	if err != nil {
		return 0, err
	} else {
		answer, _ := res.LastInsertId()
		return answer, nil
	}

}
func getRow(db *sql.DB, table TableInfo, id interface{}) (map[string]interface{}, error) {
	query := fmt.Sprintf("select * from %s where %s = ?", table.Name, table.Id)
	row := db.QueryRow(query, id)
	result := table.prepareRow()
	err := row.Scan(result...)
	if err != nil {
		return nil, err
	}
	return table.transformRow(result), nil
}

func initContext(db *sql.DB) (*TablesContext, error) {
	tables, err := getTables(db)
	if err != nil {
		return nil, err
	}
	result := new(TablesContext)
	result.TableNames = tables
	result.Tables = make(map[string]TableInfo, len(tables))
	for _, table := range tables {
		rows, err := db.Query(`SELECT column_name, 
			       IF(column_key = 'PRI', true, false) AS 'key', 
			       DATA_TYPE, 
			       IF(is_nullable = 'NO', true, false) AS nullable 
			FROM information_schema.columns 
			WHERE table_name = ? 
			  AND table_schema = database() 
			ORDER BY ORDINAL_POSITION`, table)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var keyName string
		fields := make([]FieldInfo, 0)
		for rows.Next() {
			f := new(FieldInfo)
			rows.Scan(&f.Name, &f.IsKey, &f.Type, &f.Required)
			if f.IsKey {
				keyName = f.Name
			}
			fields = append(fields, *f)
		}
		result.Tables[table] = TableInfo{
			Name:   table,
			Id:     keyName,
			Fields: fields,
		}
	}
	return result, nil
}

func getRows(db *sql.DB, table TableInfo, limit int, offset int) ([]interface{}, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM %s LIMIT %d OFFSET %d", table.Name, limit, offset))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := []interface{}{}
	for rows.Next() {
		row := table.prepareRow()
		rows.Scan(row...)
		result = append(result, table.transformRow(row))
	}
	return result, nil
}

func getTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var tableName string
		rows.Scan(&tableName)
		tables = append(tables, tableName)
	}
	return tables, nil
}
