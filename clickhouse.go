package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"time"

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/hashicorp/go-version"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

type Config struct {
	DriverName                string
	DSN                       string
	Conn                      gorm.ConnPool
	DisableDatetimePrecision  bool
	DontSupportRenameColumn   bool
	SkipInitializeWithVersion bool
	DefaultGranularity        int    // 1 granule = 8192 rows
	DefaultCompression        string // default compression algorithm. LZ4 is lossless
	DefaultIndexType          string // index stores extremes of the expression
	DefaultTableEngineOpts    string
	PoolMaxIdleCount          int           // zero means defaultMaxIdleConns; negative means 0
	PoolMaxOpenCount          int           // <= 0 means unlimited
	PoolMaxLifetime           time.Duration // maximum amount of time a connection may be reused
	PoolMaxIdleTime           time.Duration // maximum amount of time a connection may be idle before being closed
}

type Dialector struct {
	*Config
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{Config: &Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (d Dialector) Name() string {
	return "clickhouse"
}

func (d Dialector) Initialize(db *gorm.DB) (err error) {
	// register callbacks
	ctx := context.Background()
	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		DeleteClauses: []string{"DELETE", "WHERE"},
	})
	db.Callback().Create().Replace("gorm:create", Create)

	// assign option fields to default values
	if d.DriverName == "" {
		d.DriverName = "clickhouse"
	}

	// default settings
	if d.Config.DefaultGranularity == 0 {
		d.Config.DefaultGranularity = 3
	}

	if d.Config.DefaultCompression == "" {
		d.Config.DefaultCompression = "LZ4"
	}

	if d.DefaultIndexType == "" {
		d.DefaultIndexType = "minmax"
	}

	if d.DefaultTableEngineOpts == "" {
		d.DefaultTableEngineOpts = "ENGINE=MergeTree() ORDER BY tuple()"
	}

	if d.Conn != nil {
		db.ConnPool = d.Conn
	} else {
		var dbb *sql.DB

		dbb, err = sql.Open(d.DriverName, d.DSN)
		if err != nil {
			return err
		}

		if d.PoolMaxIdleTime > 0 {
			dbb.SetConnMaxIdleTime(d.PoolMaxIdleTime)
		}

		if d.PoolMaxLifetime > 0 {
			dbb.SetConnMaxLifetime(d.PoolMaxLifetime)
		}

		if d.PoolMaxIdleCount > 0 {
			dbb.SetMaxIdleConns(d.PoolMaxIdleCount)
		}

		if d.PoolMaxOpenCount > 0 {
			dbb.SetMaxOpenConns(d.PoolMaxOpenCount)
		}

		db.ConnPool = dbb
	}

	if !d.SkipInitializeWithVersion {
		var vs string
		err = db.ConnPool.QueryRowContext(ctx, "SELECT version()").Scan(&vs)
		if err != nil {
			return err
		}
		dbversion, _ := version.NewVersion(vs)
		versionNoRenameColumn, _ := version.NewConstraint("< 20.4")

		if versionNoRenameColumn.Check(dbversion) {
			d.Config.DontSupportRenameColumn = true
		}
	}

	for k, v := range d.ClauseBuilders() {
		db.ClauseBuilders[k] = v
	}
	return
}

func modifyStatementWhereConds(stmt *gorm.Statement) {
	if c, ok := stmt.Clauses["WHERE"]; ok {
		if where, ok := c.Expression.(clause.Where); ok {
			modifyExprs(where.Exprs)
		}
	}
}

func modifyExprs(exprs []clause.Expression) {
	for idx, expr := range exprs {
		switch v := expr.(type) {
		case clause.AndConditions:
			modifyExprs(v.Exprs)
		case clause.NotConditions:
			modifyExprs(v.Exprs)
		case clause.OrConditions:
			modifyExprs(v.Exprs)
		default:
			reflectValue := reflect.ValueOf(expr)
			if reflectValue.Kind() == reflect.Struct {
				if field := reflectValue.FieldByName("Column"); field.IsValid() && !field.IsZero() {
					if column, ok := field.Interface().(clause.Column); ok {
						column.Table = ""
						result := reflect.New(reflectValue.Type()).Elem()
						result.Set(reflectValue)
						result.FieldByName("Column").Set(reflect.ValueOf(column))
						exprs[idx] = result.Interface().(clause.Expression)
					}
				}
			}
		}
	}
}

func (d Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	clauseBuilders := map[string]clause.ClauseBuilder{
		"DELETE": func(c clause.Clause, builder clause.Builder) {
			builder.WriteString("ALTER TABLE ")

			var addedTable bool
			if stmt, ok := builder.(*gorm.Statement); ok {
				if c, ok := stmt.Clauses["FROM"]; ok {
					addedTable = true
					c.Name = ""
					c.Build(builder)
				}
				modifyStatementWhereConds(stmt)
			}

			if !addedTable {
				builder.WriteQuoted(clause.Table{Name: clause.CurrentTable})
			}
			builder.WriteString(" DELETE")
		},
		"UPDATE": func(c clause.Clause, builder clause.Builder) {
			builder.WriteString("ALTER TABLE ")

			var addedTable bool
			if stmt, ok := builder.(*gorm.Statement); ok {
				if c, ok := stmt.Clauses["FROM"]; ok {
					addedTable = true
					c.Name = ""
					c.Build(builder)
				}
				modifyStatementWhereConds(stmt)
			}

			if !addedTable {
				builder.WriteQuoted(clause.Table{Name: clause.CurrentTable})
			}
			builder.WriteString(" UPDATE")
		},
		"SET": func(c clause.Clause, builder clause.Builder) {
			c.Name = ""
			c.Build(builder)
		},
	}

	return clauseBuilders
}

func (d Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{
		Migrator: migrator.Migrator{
			Config: migrator.Config{
				DB:        db,
				Dialector: d,
			},
		},
		Dialector: d,
	}
}

func (d Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "UInt8"
	case schema.Int, schema.Uint:
		sqlType := "Int64"
		switch {
		case field.Size <= 8:
			sqlType = "Int8"
		case field.Size <= 16:
			sqlType = "Int16"
		case field.Size <= 32:
			sqlType = "Int32"
		}
		if field.DataType == schema.Uint {
			sqlType = "U" + sqlType
		}
		return sqlType
	case schema.Float:
		if field.Precision > 0 {
			return fmt.Sprintf("decimal(%d, %d)", field.Precision, field.Scale)
		}
		if field.Size <= 32 {
			return "Float32"
		}
		return "Float64"
	case schema.String:
		if field.Size == 0 {
			return "String"
		}
		return fmt.Sprintf("FixedString(%d)", field.Size)
	case schema.Bytes:
		return "String"
	case schema.Time:
		// TODO: support TimeZone
		precision := ""
		if !d.DisableDatetimePrecision {
			if field.Precision == 0 {
				field.Precision = 3
			}
			if field.Precision > 0 {
				precision = fmt.Sprintf("(%d)", field.Precision)
			}
		}
		return "DateTime64" + precision
	}

	return string(field.DataType)
}

func (d Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (d Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v interface{}) {
	writer.WriteByte('?')
}

func (d Dialector) QuoteTo(writer clause.Writer, str string) {
	writer.WriteByte('`')
	if strings.Contains(str, ".") {
		for idx, str := range strings.Split(str, ".") {
			if idx > 0 {
				writer.WriteString(".`")
			}
			writer.WriteString(str)
			writer.WriteByte('`')
		}
	} else {
		writer.WriteString(str)
		writer.WriteByte('`')
	}
}

func (d Dialector) Explain(sql string, vars ...interface{}) string {
	return logger.ExplainSQL(sql, nil, `'`, vars...)
}

func (d Dialector) SavePoint(tx *gorm.DB, name string) error {
	return gorm.ErrUnsupportedDriver
}

func (d Dialector) RollbackTo(tx *gorm.DB, name string) error {
	return gorm.ErrUnsupportedDriver
}
