package csvexport_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go/aws"

	"github.com/spring-media/curation-pkgs-public/pkg/csvexport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var dynamoMockRespV2 = []map[string]interface{}{
	{
		"ArticleLastUpdated": "2022-04-27T08:36:48.386Z",
		"Block":              "Meldungen1",
		"Categories": []interface{}{
			map[string]interface{}{
				"name":  "money_business1",
				"score": 0.958231,
			},
		},
		"EntriesAveragedFromHome": map[string]interface{}{
			"15":  1.333333,
			"180": 2.861111,
			"30":  3.166667,
			"45":  4.888889,
			"5":   5.000000,
		},
		"IsPremium":   false,
		"IsSponsored": false,
		"Meta": map[string]interface{}{
			"Department":       "wirtschaft",
			"FromInvestigativ": false,
			"FromNewsteam":     true,
			"HasVideo":         false,
		},
		"PerformanceLastUpdated": "2022-04-27T13:10:30Z",
	},
	{
		"ArticleLastUpdated": "2022-04-28T08:36:48.386Z",
		"Block":              "Meldungen2",
		"Categories": []interface{}{
			map[string]interface{}{
				"name":  "money_business2",
				"score": 0.99,
			},
		},
		"EntriesAveragedFromHome": map[string]interface{}{
			"15":  6.333333,
			"180": 7.861111,
			"30":  8.166667,
			"45":  9.888889,
			"5":   10.000000,
		},
		"IsPremium":   true,
		"IsSponsored": true,
		"Meta": map[string]interface{}{
			"Department":       "wirtschaft2",
			"FromInvestigativ": true,
			"FromNewsteam":     false,
			"HasVideo":         true,
		},
		"PerformanceLastUpdated": "2022-04-28T13:10:30Z",
	},
}

func newMockScanV2(t *testing.T, resp []map[string]interface{}) *mockScanV2 {
	t.Helper()
	var items []map[string]types.AttributeValue
	for _, item := range resp {
		itemMap, err := attributevalue.MarshalMap(item)
		require.NoError(t, err)
		items = append(items, itemMap)
	}
	return &mockScanV2{
		resp: dynamodb.ScanOutput{
			Items: items,
		},
	}
}

type mockScanV2 struct {
	requests []dynamodb.ScanInput
	resp     dynamodb.ScanOutput
}

func (d *mockScanV2) Scan(ctx context.Context, params *dynamodb.ScanInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ScanOutput, error) {
	d.requests = append(d.requests, *params)
	return &d.resp, nil
}

func TestDynamoToCSVV2(t *testing.T) {
	t.Parallel()
	option := csvexport.ScanOptionV2{}

	db := newMockScanV2(t, dynamoMockRespV2)
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), option)

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,Block,Categories,EntriesAveragedFromHome,IsPremium,IsSponsored,Meta,PerformanceLastUpdated
2022-04-27T08:36:48.386Z,Meldungen1,"[{""name"":""money_business1"",""score"":0.958231}]","{""15"":1.333333,""180"":2.861111,""30"":3.166667,""45"":4.888889,""5"":5}",false,false,"{""Department"":""wirtschaft"",""FromInvestigativ"":false,""FromNewsteam"":true,""HasVideo"":false}",2022-04-27T13:10:30Z
2022-04-28T08:36:48.386Z,Meldungen2,"[{""name"":""money_business2"",""score"":0.99}]","{""15"":6.333333,""180"":7.861111,""30"":8.166667,""45"":9.888889,""5"":10}",true,true,"{""Department"":""wirtschaft2"",""FromInvestigativ"":true,""FromNewsteam"":false,""HasVideo"":true}",2022-04-28T13:10:30Z
`
	assert.Equal(t, expectedCSV, string(b))
}

func TestDynamoToCSVWithColsV2(t *testing.T) {
	t.Parallel()
	db := newMockScanV2(t, dynamoMockRespV2)
	cols := csvexport.Columns{
		csvexport.Column{Name: "ArticleLastUpdated"},
		csvexport.Column{Name: "Block", OverwriteValue: true, OverwriteWithValue: ""},
		csvexport.Column{Name: "Categories", OverwriteValue: true, OverwriteWithValue: nil},
		csvexport.Column{Name: "EntriesAveragedFromHome", OverwriteValue: true, OverwriteWithValue: 3},
		csvexport.Column{Name: "IsPremium", OverwriteValue: true, OverwriteWithValue: 1.33},
		csvexport.Column{Name: "IsSponsored", OverwriteValue: true, OverwriteWithValue: false},
		csvexport.Column{Name: "PerformanceLastUpdated"},
	}
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), csvexport.ScanOptionV2{}, csvexport.WithColumns(cols))

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,Block,Categories,EntriesAveragedFromHome,IsPremium,IsSponsored,PerformanceLastUpdated
2022-04-27T08:36:48.386Z,,null,3,1.33,false,2022-04-27T13:10:30Z
2022-04-28T08:36:48.386Z,,null,3,1.33,false,2022-04-28T13:10:30Z
`
	assert.Equal(t, expectedCSV, string(b))
}

func TestDynamoToCSVWithColsTargetNameV2(t *testing.T) {
	t.Parallel()
	db := newMockScanV2(t, dynamoMockRespV2)
	cols := csvexport.Columns{
		csvexport.Column{Name: "ArticleLastUpdated"},
		csvexport.Column{Name: "PerformanceLastUpdated", TargetName: "PerformanceUpdatedLast"},
	}
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), csvexport.ScanOptionV2{}, csvexport.WithColumns(cols))

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,PerformanceUpdatedLast
2022-04-27T08:36:48.386Z,2022-04-27T13:10:30Z
2022-04-28T08:36:48.386Z,2022-04-28T13:10:30Z
`
	assert.Equal(t, expectedCSV, string(b))
}

func TestDynamoToCSVWithColsValueFuncV2(t *testing.T) {
	t.Parallel()
	db := newMockScanV2(t, dynamoMockRespV2)

	valueFn := func(val interface{}) (string, error) {
		v, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("failed to cast to string")
		}

		if v == "Meldungen1" {
			return "Meldungen123", nil
		}

		return v, nil
	}

	valueFnNew := func(val interface{}) (string, error) {
		v, ok := val.(string)
		if !ok {
			return "", fmt.Errorf("failed to cast to string")
		}

		if v == "Meldungen2" {
			return "Meldungen999", nil
		}

		return v, nil
	}
	cols := csvexport.Columns{
		csvexport.Column{Name: "ArticleLastUpdated"},
		csvexport.Column{Name: "Block", ValueFunc: valueFn},
		csvexport.Column{Name: "NewBlock", ValueFunc: valueFnNew, ValueFuncCol: "Block"},
	}
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), csvexport.ScanOptionV2{}, csvexport.WithColumns(cols))

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,Block,NewBlock
2022-04-27T08:36:48.386Z,Meldungen123,Meldungen1
2022-04-28T08:36:48.386Z,Meldungen2,Meldungen999
`
	assert.Equal(t, expectedCSV, string(b))
}

func TestScanOptions(t *testing.T) {
	t.Parallel()
	db := newMockScanV2(t, dynamoMockRespV2)
	cols := csvexport.Columns{
		csvexport.Column{Name: "ArticleLastUpdated"},
	}
	scanOpt := csvexport.ScanOptionV2{
		TableName:        "test-table",
		FilterExpression: "Block = :block",
		ExpressionAttrValues: map[string]types.AttributeValue{
			":block": &types.AttributeValueMemberS{Value: "testBlock"},
		},
		ExpressionAttrNames: map[string]string{"#block": "Block"},
	}

	_, err := csvexport.DynamoToCSVV2(db, context.Background(), scanOpt, csvexport.WithColumns(cols))
	require.Nil(t, err)
	require.Len(t, db.requests, 1)
	assert.Equal(t, dynamodb.ScanInput{
		TableName:        aws.String(scanOpt.TableName),
		FilterExpression: aws.String(scanOpt.FilterExpression),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":block": &types.AttributeValueMemberS{Value: "testBlock"},
		},
		ExpressionAttributeNames: map[string]string{"#block": "Block"},
	}, db.requests[0])
}
