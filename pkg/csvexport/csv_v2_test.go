package csvexport_test

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"testing"

	"github.com/spring-media/curation-pkgs-public/pkg/csvexport"
	"github.com/stretchr/testify/assert"
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

type mockScanV2 struct {
	resp []map[string]interface{}
}

func (d mockScanV2) Scan(ctx context.Context, opt csvexport.ScanOption, startKey map[string]types.AttributeValue) ([]map[string]interface{}, map[string]types.AttributeValue, error) {
	return d.resp, map[string]types.AttributeValue{}, nil
}

func TestDynamoToCSVV2(t *testing.T) {
	t.Parallel()
	option := csvexport.ScanOption{}

	db := mockScanV2{resp: dynamoMockRespV2}
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
	db := mockScanV2{resp: dynamoMockRespV2}
	cols := csvexport.Columns{
		csvexport.Column{Name: "ArticleLastUpdated"},
		csvexport.Column{Name: "Block", OverwriteValue: true, OverwriteWithValue: ""},
		csvexport.Column{Name: "Categories", OverwriteValue: true, OverwriteWithValue: nil},
		csvexport.Column{Name: "EntriesAveragedFromHome", OverwriteValue: true, OverwriteWithValue: 3},
		csvexport.Column{Name: "IsPremium", OverwriteValue: true, OverwriteWithValue: 1.33},
		csvexport.Column{Name: "IsSponsored", OverwriteValue: true, OverwriteWithValue: false},
		csvexport.Column{Name: "PerformanceLastUpdated"},
	}
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), csvexport.ScanOption{}, csvexport.WithColumns(cols))

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,Block,Categories,EntriesAveragedFromHome,IsPremium,IsSponsored,PerformanceLastUpdated
2022-04-27T08:36:48.386Z,,null,3,1.33,false,2022-04-27T13:10:30Z
2022-04-28T08:36:48.386Z,,null,3,1.33,false,2022-04-28T13:10:30Z
`
	assert.Equal(t, expectedCSV, string(b))
}

func TestDynamoToCSVWithColsTargetNameV2(t *testing.T) {
	t.Parallel()
	db := mockScanV2{resp: dynamoMockRespV2}
	cols := csvexport.Columns{
		csvexport.Column{Name: "ArticleLastUpdated"},
		csvexport.Column{Name: "PerformanceLastUpdated", TargetName: "PerformanceUpdatedLast"},
	}
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), csvexport.ScanOption{}, csvexport.WithColumns(cols))

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,PerformanceUpdatedLast
2022-04-27T08:36:48.386Z,2022-04-27T13:10:30Z
2022-04-28T08:36:48.386Z,2022-04-28T13:10:30Z
`
	assert.Equal(t, expectedCSV, string(b))
}

func TestDynamoToCSVWithColsValueFuncV2(t *testing.T) {
	t.Parallel()
	db := mockScanV2{resp: dynamoMockRespV2}

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
	b, err := csvexport.DynamoToCSVV2(db, context.Background(), csvexport.ScanOption{}, csvexport.WithColumns(cols))

	assert.Nil(t, err)

	expectedCSV := `ArticleLastUpdated,Block,NewBlock
2022-04-27T08:36:48.386Z,Meldungen123,Meldungen1
2022-04-28T08:36:48.386Z,Meldungen2,Meldungen999
`
	assert.Equal(t, expectedCSV, string(b))
}
