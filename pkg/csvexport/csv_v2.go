package csvexport

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StorageV2 interface {
	Scan(ctx context.Context, opt ScanOption, startKey map[string]types.AttributeValue) ([]map[string]interface{}, map[string]types.AttributeValue, error)
}

type DynoStorageV2 struct {
	DDB *dynamodb.Client
}

func DynamoToCSVV2(db StorageV2, ctx context.Context, scanOpt ScanOption, opts ...Option) ([]byte, error) {
	var b bytes.Buffer

	w := csv.NewWriter(&b)

	defer w.Flush()

	var startKey map[string]types.AttributeValue

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
					record = append(record, removeNewLines(val))
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

func (db DynoStorageV2) Scan(ctx context.Context, opt ScanOption, startKey map[string]types.AttributeValue) ([]map[string]interface{}, map[string]types.AttributeValue, error) {
	var expressionAttributeValues map[string]types.AttributeValue
	if opt.ExpressionAttrValues != "" {
		if err := json.Unmarshal([]byte(opt.ExpressionAttrValues), &expressionAttributeValues); err != nil {
			return nil, nil, fmt.Errorf("expression attribute values invalid: %w", err)
		}
	}

	var expressionAttributeNames map[string]string
	if opt.ExpressionAttrNames != "" {
		expressionAttributeNames = make(map[string]string)
		if err := json.Unmarshal([]byte(opt.ExpressionAttrNames), &expressionAttributeNames); err != nil {
			return nil, nil, fmt.Errorf("expression attribute names invalid: %w", err)
		}
	}

	var filterExpression *string
	if opt.FilterExpression != "" {
		filterExpression = aws.String(opt.FilterExpression)
	}

	out, err := db.DDB.Scan(ctx, &dynamodb.ScanInput{
		TableName:                 aws.String(opt.TableName),
		ExclusiveStartKey:         startKey,
		ExpressionAttributeNames:  expressionAttributeNames,
		FilterExpression:          filterExpression,
		ExpressionAttributeValues: expressionAttributeValues,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("db.Scan: %w", err)
	}

	var resp []map[string]interface{}
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &resp); err != nil {
		return nil, nil, fmt.Errorf("dynamodb unmarshal list of maps: %w", err)
	}

	return resp, out.LastEvaluatedKey, nil
}

type S3PutClient interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
}

func UploadToS3V2(ctx context.Context, b []byte, client S3PutClient, bucket, path, fname string) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Body:   bytes.NewReader(b),
		Bucket: aws.String(bucket),
		Key:    aws.String(path + fname),
	})
	if err != nil {
		return fmt.Errorf("failed to PutObject %s to S3: %w", fname, err)
	}

	return nil
}
