package dyngeo

import (
	"context"
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/imdario/mergo"
)

type db struct {
	config DynGeoConfig
}

func newDB(config DynGeoConfig) db {
	return db{
		config: config,
	}
}

func (db db) queryGeoHash(ctx context.Context, queryInput dynamodb.QueryInput, hashKey uint64, ghr geoHashRange, limit int) []*dynamodb.QueryOutput {
	queryOutputs := []*dynamodb.QueryOutput{}

	hash, err := attributevalue.Marshal(hashKey)
	if err != nil {
		log.Println(err)
		return queryOutputs
	}

	rangeMin, err := attributevalue.Marshal(ghr.rangeMin)
	if err != nil {
		log.Println(err)
		return queryOutputs
	}

	rangeMax, err := attributevalue.Marshal(ghr.rangeMax)
	if err != nil {
		log.Println(err)
		return queryOutputs
	}

	keyConditions := map[string]types.Condition{
		db.config.HashKeyAttributeName: {
			ComparisonOperator: types.ComparisonOperatorEq,
			AttributeValueList: []types.AttributeValue{
				hash,
			},
		},
		db.config.GeoHashAttributeName: {
			ComparisonOperator: types.ComparisonOperatorBetween,
			AttributeValueList: []types.AttributeValue{
				rangeMin,
				rangeMax,
			},
		},
	}

	defaultInput := dynamodb.QueryInput{
		TableName:              aws.String(db.config.TableName),
		KeyConditions:          keyConditions,
		IndexName:              aws.String(db.config.GeoHashIndexName),
		ConsistentRead:         aws.Bool(db.config.ConsistentRead),
		ReturnConsumedCapacity: types.ReturnConsumedCapacityTotal,
	}

	if err := mergo.Merge(&queryInput, defaultInput); err != nil {
		log.Println(err)
		return nil
	}

	output, queryOutputs := db.paginateQuery(ctx, queryInput, queryOutputs)

	// if you provide a limit less than 0 - it means you want to loop for ALL entries in the range
	// otherwise - loop until reach the limit, or you hit the end of the query
	iteration := 1
	iterationUntilDone := limit < 0
	for iterationUntilDone || iteration < limit {
		if output.LastEvaluatedKey == nil {
			break
		}

		queryInput.ExclusiveStartKey = output.LastEvaluatedKey
		output, queryOutputs = db.paginateQuery(ctx, queryInput, queryOutputs)
		iteration++
	}

	return queryOutputs
}

func (db db) paginateQuery(ctx context.Context, queryInput dynamodb.QueryInput, queryOutputs []*dynamodb.QueryOutput) (*dynamodb.QueryOutput, []*dynamodb.QueryOutput) {
	output, err := db.config.DynamoDBClient.Query(ctx, &queryInput)
	if err != nil {
		log.Println(err)
	}
	queryOutputs = append(queryOutputs, output)
	return output, queryOutputs
}

func (db db) getPoint(ctx context.Context, input GetPointInput) (*GetPointOutput, error) {
	_, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	hash, err := attributevalue.Marshal(hashKey)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	rng, err := attributevalue.Marshal(input.RangeKeyValue)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	getItemInput := input.GetItemInput
	getItemInput.TableName = aws.String(db.config.TableName)
	getItemInput.Key = map[string]types.AttributeValue{
		db.config.HashKeyAttributeName:  hash,
		db.config.RangeKeyAttributeName: rng,
	}

	out, err := db.config.DynamoDBClient.GetItem(ctx, &getItemInput)

	return &GetPointOutput{out}, err
}

func (db db) putPoint(ctx context.Context, input PutPointInput) (*PutPointOutput, error) {
	geoHash, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	hash, err := attributevalue.Marshal(hashKey)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	rng, err := attributevalue.Marshal(input.RangeKeyValue)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	geo, err := attributevalue.Marshal(geoHash)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	putItemInput := input.PutItemInput
	putItemInput.TableName = aws.String(db.config.TableName)
	putItemInput.Item = input.PutItemInput.Item
	putItemInput.Item[db.config.HashKeyAttributeName] = hash
	putItemInput.Item[db.config.RangeKeyAttributeName] = rng
	putItemInput.Item[db.config.GeoHashAttributeName] = geo

	jsonAttr, err := json.Marshal(newGeoJSONAttribute(input.GeoPoint, db.config.LongitudeFirst))
	if err != nil {
		return nil, err
	}

	jsonAttrVal, err := attributevalue.Marshal(jsonAttr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	putItemInput.Item[db.config.GeoJSONAttributeName] = jsonAttrVal

	out, err := db.config.DynamoDBClient.PutItem(ctx, &putItemInput)

	return &PutPointOutput{out}, err
}

func (db db) batchWritePoints(ctx context.Context, inputs []PutPointInput) (*BatchWritePointOutput, error) {
	writeInputs := []types.WriteRequest{}
	for _, input := range inputs {
		geoHash, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

		geo, err := attributevalue.Marshal(geoHash)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		hash, err := attributevalue.Marshal(hashKey)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		rng, err := attributevalue.Marshal(input.RangeKeyValue)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		putItemInput := input.PutItemInput
		putRequest := types.PutRequest{
			Item: putItemInput.Item,
		}
		putRequest.Item[db.config.HashKeyAttributeName] = hash
		putRequest.Item[db.config.RangeKeyAttributeName] = rng
		putRequest.Item[db.config.GeoHashAttributeName] = geo

		jsonAttr, err := json.Marshal(newGeoJSONAttribute(input.GeoPoint, db.config.LongitudeFirst))
		if err != nil {
			return nil, err
		}

		jsonAttrVal, err := attributevalue.Marshal(jsonAttr)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		putRequest.Item[db.config.GeoJSONAttributeName] = jsonAttrVal

		writeInputs = append(writeInputs, types.WriteRequest{PutRequest: &putRequest})
	}

	bachWriteItemInput := &dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]types.WriteRequest{
			db.config.TableName: writeInputs,
		},
	}

	out, err := db.config.DynamoDBClient.BatchWriteItem(ctx, bachWriteItemInput)
	return &BatchWritePointOutput{out}, err
}

func (db db) updatePoint(ctx context.Context, input UpdatePointInput) (*UpdatePointOutput, error) {
	_, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	hash, err := attributevalue.Marshal(hashKey)
	if err != nil {
		return nil, err
	}

	rgk, err := attributevalue.Marshal(input.RangeKeyValue)
	if err != nil {
		return nil, err
	}

	input.UpdateItemInput.TableName = aws.String(db.config.TableName)
	if input.UpdateItemInput.Key == nil {
		input.UpdateItemInput.Key = map[string]types.AttributeValue{
			db.config.HashKeyAttributeName:  hash,
			db.config.RangeKeyAttributeName: rgk,
		}
	}

	// geoHash and geoJSON cannot be updated
	if input.UpdateItemInput.AttributeUpdates != nil {
		delete(input.UpdateItemInput.AttributeUpdates, db.config.GeoHashAttributeName)
		delete(input.UpdateItemInput.AttributeUpdates, db.config.GeoJSONAttributeName)
	}

	out, err := db.config.DynamoDBClient.UpdateItem(ctx, &input.UpdateItemInput)

	return &UpdatePointOutput{out}, err
}

func (db db) deletePoint(ctx context.Context, input DeletePointInput) (*DeletePointOutput, error) {
	_, hashKey := generateHashes(input.GeoPoint, db.config.HashKeyLength)

	hash, err := attributevalue.Marshal(hashKey)
	if err != nil {
		return nil, err
	}

	rgk, err := attributevalue.Marshal(input.RangeKeyValue)
	if err != nil {
		return nil, err
	}

	deleteItemInput := input.DeleteItemInput
	deleteItemInput.TableName = aws.String(db.config.TableName)
	deleteItemInput.Key = map[string]types.AttributeValue{
		db.config.HashKeyAttributeName:  hash,
		db.config.RangeKeyAttributeName: rgk,
	}

	out, err := db.config.DynamoDBClient.DeleteItem(ctx, &deleteItemInput)

	return &DeletePointOutput{out}, err
}

func GetCreateTableRequest(config DynGeoConfig) *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		TableName: aws.String(config.TableName),
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(5),
		},
		KeySchema: []types.KeySchemaElement{
			{
				KeyType:       types.KeyTypeHash,
				AttributeName: aws.String(config.HashKeyAttributeName),
			},
			{
				KeyType:       types.KeyTypeRange,
				AttributeName: aws.String(config.RangeKeyAttributeName),
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(config.HashKeyAttributeName),
				AttributeType: types.ScalarAttributeTypeN,
			},
			{
				AttributeName: aws.String(config.RangeKeyAttributeName),
				AttributeType: types.ScalarAttributeTypeS,
			},
			{
				AttributeName: aws.String(config.GeoHashAttributeName),
				AttributeType: types.ScalarAttributeTypeN,
			},
		},
		LocalSecondaryIndexes: []types.LocalSecondaryIndex{
			{
				IndexName: aws.String(config.GeoHashIndexName),
				KeySchema: []types.KeySchemaElement{
					{
						KeyType:       types.KeyTypeHash,
						AttributeName: aws.String(config.HashKeyAttributeName),
					},
					{
						KeyType:       types.KeyTypeRange,
						AttributeName: aws.String(config.GeoHashAttributeName),
					},
				},
				Projection: &types.Projection{
					ProjectionType: types.ProjectionTypeAll,
				},
			},
		},
	}
}
