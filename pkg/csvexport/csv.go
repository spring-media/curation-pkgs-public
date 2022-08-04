package csvexport

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
)

type Storage interface {
	Scan(ctx context.Context, opt ScanOption, startKey map[string]*dynamodb.AttributeValue) ([]map[string]interface{}, map[string]*dynamodb.AttributeValue, error)
}

type DynoStorage struct {
	DDB dynamodbiface.DynamoDBAPI
}

type ScanOption struct {
	TableName            string
	FilterExpression     string
	ExpressionAttrNames  string
	ExpressionAttrValues string
}

type Columns []Column

func (cols Columns) ValueFunc(colName string) (ValueFunc, string, bool) {
	for i, c := range cols {
		if c.Name != colName {
			continue
		}

		if cols[i].ValueFunc != nil {
			return cols[i].ValueFunc, cols[i].ValueFuncCol, true
		}

		return nil, "", false
	}

	return nil, "", false
}

type Column struct {
	Name string
	// If TargetName is set, Name gets renamed to TargetName in header
	TargetName string
	// If OverwriteValue is set to true, all values of the column are set to OverwriteWithValue.
	OverwriteValue     bool
	OverwriteWithValue interface{}
	// Function to process value in col and use that as result
	ValueFunc    ValueFunc
	ValueFuncCol string
}

type ValueFunc func(v interface{}) (string, error)

type CSVExporter struct {
	cols Columns
}

type Option func(c *CSVExporter)

func WithColumns(cols Columns) Option {
	return func(c *CSVExporter) {
		c.cols = cols
	}
}

func DynamoToCSV(db Storage, ctx context.Context, scanOpt ScanOption, opts ...Option) ([]byte, error) {
	var b bytes.Buffer

	w := csv.NewWriter(&b)

	defer w.Flush()

	var startKey map[string]*dynamodb.AttributeValue

	var csvExp CSVExporter

	var keyOrder []string
	var header []string

	for _, opt := range opts {
		opt(&csvExp)
	}

	count := 0

	for {
		resp, sk, err := db.Scan(ctx, scanOpt, startKey)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}

		for _, attr := range resp {
			if count == 0 {
				for _, v := range csvExp.cols {
					keyOrder = append(keyOrder, v.Name)

					headerName := v.Name

					if v.TargetName != "" {
						headerName = v.TargetName
					}

					header = append(header, headerName)
				}

				if csvExp.cols == nil {
					for k := range attr {
						keyOrder = append(keyOrder, k)
					}

					sort.Strings(keyOrder)

					header = keyOrder
				}

				_ = w.Write(header)
			}

			record := make([]string, 0, len(keyOrder))

			for i, k := range keyOrder {
				value := attr[k]

				// Empty Value of column?
				if len(csvExp.cols) > 0 && csvExp.cols[i].OverwriteValue {
					value = csvExp.cols[i].OverwriteWithValue
				}

				// Custom function?
				valueFn, valueFnCol, ok := csvExp.cols.ValueFunc(k)

				if ok {
					if valueFnCol != "" {
						value = attr[valueFnCol]
					}
					newVal, err := valueFn(value)
					if err != nil {
						return nil, fmt.Errorf("failed to process custom valueFunction on column %s: %w", valueFnCol, err)
					}
					record = append(record, newVal)
					continue
				}

				switch val := value.(type) {
				case float64:
					// protect exponential notation layout
					record = append(record, strconv.FormatFloat(val, 'f', -1, 64))
				case string:
					record = append(record, val)
				default:
					js, err := json.Marshal(value)
					if err != nil {
						return nil, fmt.Errorf("failed to marshal value: %w", err)
					}

					record = append(record, string(js))
				}
			}

			_ = w.Write(record)
			count++
		}

		startKey = sk
		if len(startKey) == 0 {
			break
		}
	}

	w.Flush()

	return b.Bytes(), nil
}

func (db DynoStorage) Scan(ctx context.Context, opt ScanOption, startKey map[string]*dynamodb.AttributeValue) ([]map[string]interface{}, map[string]*dynamodb.AttributeValue, error) {
	var expressionAttributeValues map[string]*dynamodb.AttributeValue
	if opt.ExpressionAttrValues != "" {
		if err := json.Unmarshal([]byte(opt.ExpressionAttrValues), &expressionAttributeValues); err != nil {
			return nil, nil, fmt.Errorf("expression attribute values invalid: %w", err)
		}
	}

	var expressionAttributeNames map[string]*string
	if opt.ExpressionAttrNames != "" {
		expressionAttributeNames = make(map[string]*string)
		if err := json.Unmarshal([]byte(opt.ExpressionAttrNames), &expressionAttributeNames); err != nil {
			return nil, nil, fmt.Errorf("expression attribute names invalid: %w", err)
		}
	}

	var filterExpression *string
	if opt.FilterExpression != "" {
		filterExpression = aws.String(opt.FilterExpression)
	}

	out, err := db.DDB.ScanWithContext(ctx, &dynamodb.ScanInput{
		TableName:                 aws.String(opt.TableName),
		ExclusiveStartKey:         startKey,
		ExpressionAttributeNames:  expressionAttributeNames,
		FilterExpression:          filterExpression,
		ExpressionAttributeValues: expressionAttributeValues,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("db.ScanWithContext: %w", err)
	}

	var resp []map[string]interface{}
	if err := dynamodbattribute.UnmarshalListOfMaps(out.Items, &resp); err != nil {
		return nil, nil, fmt.Errorf("dynamodb unmarshal list of maps: %w", err)
	}

	return resp, out.LastEvaluatedKey, nil
}

func GZIPData(data []byte) ([]byte, error) {
	var b bytes.Buffer

	gz := gzip.NewWriter(&b)

	_, err := gz.Write(data)
	if err != nil {
		return nil, fmt.Errorf("failed to gz.Write: %w", err)
	}

	if err = gz.Flush(); err != nil {
		return nil, fmt.Errorf("failed to gz.Flush: %w", err)
	}

	if err = gz.Close(); err != nil {
		return nil, fmt.Errorf("failed to gz.Close: %w", err)
	}

	return b.Bytes(), nil
}

func UploadToS3(b []byte, client s3iface.S3API, bucket, path, fname string) error {
	_, err := client.PutObject(&s3.PutObjectInput{
		Body:   bytes.NewReader(b),
		Bucket: aws.String(bucket),
		Key:    aws.String(path + fname),
	})
	if err != nil {
		return fmt.Errorf("failed to PutObject %s to S3: %w", fname, err)
	}

	return nil
}
