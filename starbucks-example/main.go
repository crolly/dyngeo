package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/crolly/dyngeo"
	"github.com/gofrs/uuid"
)

var (
	dbClient *dynamodb.DynamoDB
	dg       *dyngeo.DynGeo
	err      error
)

const BATCH_SIZE = 25

type Starbucks struct {
	Position Position `json:"position"`
	Name     string   `json:"name"`
	Address  string   `json:"address"`
	Phone    string   `json:"phone"`
}

type Position struct {
	Latitude  float64 `json:"lat"`
	Longitude float64 `json:"lng"`
}

func main() {
	dbClient = dynamodb.New(session.Must(session.NewSession(&aws.Config{
		Endpoint: aws.String("http://localhost:8000"),
		Region:   aws.String("eu-central-1"),
	})))
	dg, err = dyngeo.New(dyngeo.DynGeoConfig{
		DynamoDBClient: dbClient,
		HashKeyLength:  5,
		TableName:      "coffee-shops",
	})
	if err != nil {
		panic(err)
	}

	setupTable()
	loadData()
	queryData()
}

func setupTable() {
	createTableInput := dyngeo.GetCreateTableRequest(dg.Config)
	createTableInput.ProvisionedThroughput.ReadCapacityUnits = aws.Int64(5)
	createTableOutput, err := dbClient.CreateTable(createTableInput)
	if err != nil {
		panic(err)
	}
	fmt.Println("Table created")
	fmt.Println(createTableOutput)
}

func loadData() {
	f, err := ioutil.ReadFile("starbucks_us_locations.json")
	if err != nil {
		panic(err)
	}
	coffeeShops := []Starbucks{}
	err = json.Unmarshal([]byte(f), &coffeeShops)
	if err != nil {
		panic(err)
	}

	batchInput := []dyngeo.PutPointInput{}
	for _, s := range coffeeShops {
		id, err := uuid.NewV4()
		if err != nil {
			panic(err)
		}
		input := dyngeo.PutPointInput{
			PutItemInput: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"name":    {S: aws.String(s.Name)},
					"address": {S: aws.String(s.Address)},
				},
			},
		}
		input.RangeKeyValue = id
		input.GeoPoint = dyngeo.GeoPoint{
			Latitude:  s.Position.Latitude,
			Longitude: s.Position.Longitude,
		}
		batchInput = append(batchInput, input)
	}

	batches := [][]dyngeo.PutPointInput{}
	for BATCH_SIZE < len(batchInput) {
		batchInput, batches = batchInput[BATCH_SIZE:], append(batches, batchInput[0:BATCH_SIZE:BATCH_SIZE])
	}
	batches = append(batches, batchInput)

	for count, batch := range batches {
		output, err := dg.BatchWritePoints(batch)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Batch %d written: %s", count, output)
	}
}

func queryData() {
	start := time.Now()
	sbs := []Starbucks{}

	err := dg.QueryRadius(dyngeo.QueryRadiusInput{
		CenterPoint: dyngeo.GeoPoint{
			Latitude:  40.7769099,
			Longitude: -73.9822532,
		},
		RadiusInMeter: 5000,
	}, &sbs)
	if err != nil {
		panic(err)
	}

	for _, sb := range sbs {
		fmt.Println(sb)
	}

	fmt.Print("Executed in: ")
	fmt.Println(time.Since(start))
}
