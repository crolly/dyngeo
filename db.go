package dyngeo

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/imdario/mergo"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type db struct {
	config DynGeoConfig
}

func newDB(config DynGeoConfig) db {
	return db{
		config: config,
	}
}

func (db db) queryGeoHash(queryInput dynamodb.QueryInput, hashKey uint64, ghr geoHashRange) []*dynamodb.QueryOutput {
	queryOutputs := []*dynamodb.QueryOutput{}

	keyConditions := map[string]*dynamodb.Condition{
		db.config.HashKeyAttributeName: &dynamodb.Condition{
			ComparisonOperator: aws.String("EQ"),
			AttributeValueList: []*dynamodb.AttributeValue{
				&dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))},
				// &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))},
			},
		},
		db.config.GeoHashAttributeName: &dynamodb.Condition{
			ComparisonOperator: aws.String("BETWEEN"),
			AttributeValueList: []*dynamodb.AttributeValue{
				&dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(ghr.rangeMin, 10))},
				&dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(ghr.rangeMax, 10))},
			},
		},
	}
	defaultInput := dynamodb.QueryInput{
		TableName:              aws.String(db.config.TableName),
		KeyConditions:          keyConditions,
		IndexName:              aws.String(db.config.GeoHashIndexName),
		ConsistentRead:         aws.Bool(db.config.ConsistentRead),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	if err := mergo.Merge(&queryInput, defaultInput); err != nil {
		fmt.Println(err)
	}

	output, queryOutputs := db.paginateQuery(queryInput, queryOutputs)

	for output.LastEvaluatedKey != nil {
		queryInput.ExclusiveStartKey = output.LastEvaluatedKey
		output, queryOutputs = db.paginateQuery(queryInput, queryOutputs)
	}

	return queryOutputs
}

func (db db) paginateQuery(queryInput dynamodb.QueryInput, queryOutputs []*dynamodb.QueryOutput) (*dynamodb.QueryOutput, []*dynamodb.QueryOutput) {
	output, err := db.config.DynamoDBClient.Query(&queryInput)
	if err != nil {
		fmt.Println(err)
	}
	queryOutputs = append(queryOutputs, output)

	return output, queryOutputs
}

func (db db) getPoint(input GetPointInput) (*GetPointOutput, error) {
	_, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	getItemInput := input.GetItemInput
	getItemInput.TableName = aws.String(db.config.TableName)
	getItemInput.Key = map[string]*dynamodb.AttributeValue{
		db.config.HashKeyAttributeName:  &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))},
		db.config.RangeKeyAttributeName: &dynamodb.AttributeValue{S: aws.String(input.RangeKeyValue.String())},
	}

	out, err := db.config.DynamoDBClient.GetItem(&getItemInput)

	return &GetPointOutput{out}, err
}

func (db db) putPoint(input PutPointInput) (*PutPointOutput, error) {
	geoHash, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)
	putItemInput := input.PutItemInput
	putItemInput.TableName = aws.String(db.config.TableName)
	putItemInput.Item = input.PutItemInput.Item

	putItemInput.Item[db.config.HashKeyAttributeName] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))}
	putItemInput.Item[db.config.RangeKeyAttributeName] = &dynamodb.AttributeValue{S: aws.String(input.RangeKeyValue.String())}
	putItemInput.Item[db.config.GeoHashAttributeName] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(geoHash, 10))}

	jsonAttr, err := json.Marshal(newGeoJSONAttribute(input.GeoPoint, db.config.LongitudeFirst))
	if err != nil {
		return nil, err
	}
	putItemInput.Item[db.config.GeoJSONAttributeName] = &dynamodb.AttributeValue{S: aws.String(string(jsonAttr))}

	out, err := db.config.DynamoDBClient.PutItem(&putItemInput)

	return &PutPointOutput{out}, err
}

func (db db) batchWritePoints(inputs []PutPointInput) (*BatchWritePointOutput, error) {
	writeInputs := []*dynamodb.WriteRequest{}
	for _, input := range inputs {
		geoHash, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)
		putItemInput := input.PutItemInput

		putRequest := dynamodb.PutRequest{
			Item: putItemInput.Item,
		}
		putRequest.Item[db.config.HashKeyAttributeName] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))}
		putRequest.Item[db.config.RangeKeyAttributeName] = &dynamodb.AttributeValue{S: aws.String(input.RangeKeyValue.String())}
		putRequest.Item[db.config.GeoHashAttributeName] = &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(geoHash, 10))}

		jsonAttr, err := json.Marshal(newGeoJSONAttribute(input.GeoPoint, db.config.LongitudeFirst))
		if err != nil {
			return nil, err
		}
		putRequest.Item[db.config.GeoJSONAttributeName] = &dynamodb.AttributeValue{S: aws.String(string(jsonAttr))}

		writeInputs = append(writeInputs, &dynamodb.WriteRequest{PutRequest: &putRequest})
	}

	out, err := db.config.DynamoDBClient.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			db.config.TableName: writeInputs,
		},
	})

	return &BatchWritePointOutput{out}, err
}

func (db db) updatePoint(input UpdatePointInput) (*UpdatePointOutput, error) {
	_, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	input.UpdateItemInput.TableName = aws.String(db.config.TableName)
	if input.UpdateItemInput.Key == nil {
		input.UpdateItemInput.Key = map[string]*dynamodb.AttributeValue{
			db.config.HashKeyAttributeName:  &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))},
			db.config.RangeKeyAttributeName: &dynamodb.AttributeValue{S: aws.String(input.RangeKeyValue.String())},
		}
	}

	// geoHash and geoJSON cannot be updated
	if input.UpdateItemInput.AttributeUpdates != nil {
		delete(input.UpdateItemInput.AttributeUpdates, db.config.GeoHashAttributeName)
		delete(input.UpdateItemInput.AttributeUpdates, db.config.GeoJSONAttributeName)
	}

	out, err := db.config.DynamoDBClient.UpdateItem(&input.UpdateItemInput)

	return &UpdatePointOutput{out}, err
}

func (db db) deletePoint(input DeletePointInput) (*DeletePointOutput, error) {
	_, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	deleteItemInput := input.DeleteItemInput
	deleteItemInput.TableName = aws.String(db.config.TableName)
	deleteItemInput.Key = map[string]*dynamodb.AttributeValue{
		db.config.HashKeyAttributeName:  &dynamodb.AttributeValue{N: aws.String(strconv.FormatUint(hashKey, 10))},
		db.config.RangeKeyAttributeName: &dynamodb.AttributeValue{S: aws.String(input.RangeKeyValue.String())},
	}
	out, err := db.config.DynamoDBClient.DeleteItem(&deleteItemInput)

	return &DeletePointOutput{out}, err
}

func GetCreateTableRequest(config DynGeoConfig) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName: aws.String(config.TableName),
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			&dynamodb.KeySchemaElement{
				KeyType:       aws.String("HASH"),
				AttributeName: aws.String(config.HashKeyAttributeName),
			},
			&dynamodb.KeySchemaElement{
				KeyType:       aws.String("RANGE"),
				AttributeName: aws.String(config.RangeKeyAttributeName),
			},
		},
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(config.HashKeyAttributeName),
				AttributeType: aws.String("N"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(config.RangeKeyAttributeName),
				AttributeType: aws.String("S"),
			},
			&dynamodb.AttributeDefinition{
				AttributeName: aws.String(config.GeoHashAttributeName),
				AttributeType: aws.String("N"),
			},
		},
		LocalSecondaryIndexes: []*dynamodb.LocalSecondaryIndex{
			&dynamodb.LocalSecondaryIndex{
				IndexName: aws.String(config.GeoHashIndexName),
				KeySchema: []*dynamodb.KeySchemaElement{
					&dynamodb.KeySchemaElement{
						KeyType:       aws.String("HASH"),
						AttributeName: aws.String(config.HashKeyAttributeName),
					},
					&dynamodb.KeySchemaElement{
						KeyType:       aws.String("RANGE"),
						AttributeName: aws.String(config.GeoHashAttributeName),
					},
				},
				Projection: &dynamodb.Projection{
					ProjectionType: aws.String("ALL"),
				},
			},
		},
	}
}
