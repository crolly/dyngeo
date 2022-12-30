package dyngeo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/golang/geo/s2"
)

// MERGE_THRESHOLD ...
const MERGE_THRESHOLD = 2

type DynGeoConfig struct {
	TableName             string
	ConsistentRead        bool
	HashKeyAttributeName  string
	RangeKeyAttributeName string
	GeoHashAttributeName  string
	GeoJSONAttributeName  string
	GeoHashIndexName      string
	HashKeyLength         int8
	LongitudeFirst        bool

	DynamoDBClient  *dynamodb.Client
	s2RegionCoverer s2.RegionCoverer
}

// func NewConfig(dynamoClient *dynamodb.DynamoDB, tableName string) DynGeoConfig {
// 	return DynGeoConfig{
// 		tableName:             tableName,
// 		consistentRead:        false,
// 		hashKeyAttributeName:  "hashKey",
// 		rangeKeyAttributeName: "rangeKey",
// 		geohashAttributeName:  "geohash",
// 		geoJSONAttributeName:  "geoJson",
// 		geohashIndexName:      "geohash-index",
// 		hashKeyLength:         2,
// 		longitudeFirst:        true,

// 		dynamodbClient: dynamoClient,
// 		s2RegionCoverer: s2.RegionCoverer{
// 			MinLevel: 10,
// 			MaxLevel: 10,
// 			MaxCells: 10,
// 		},
// 	}
// }

// // SetHashKeyLength ...
// func (config *DynGeoConfig) SetHashKeyLength(length int8) {
// 	config.hashKeyLength = length
// }
