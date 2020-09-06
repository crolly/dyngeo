package dyngeo

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/imdario/mergo"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/golang/geo/s2"
)

type DynGeo struct {
	Config DynGeoConfig
	db     db
}

// NewDynGeo ...
func New(config DynGeoConfig) (*DynGeo, error) {
	if config.DynamoDBClient == nil {
		return nil, errors.New("DynamoDBClient is required")
	}

	if config.TableName == "" {
		return nil, errors.New("TableName is required")
	}

	defaultConfig := DynGeoConfig{
		TableName:             config.TableName,
		ConsistentRead:        false,
		HashKeyAttributeName:  "hashKey",
		RangeKeyAttributeName: "rangeKey",
		GeoHashAttributeName:  "geohash",
		GeoJSONAttributeName:  "geoJson",
		GeoHashIndexName:      "geohash-index",
		HashKeyLength:         2,
		LongitudeFirst:        true,

		DynamoDBClient: config.DynamoDBClient,
	}

	err := mergo.Merge(&config, defaultConfig)
	if err != nil {
		return nil, err
	}

	config.s2RegionCoverer = s2.RegionCoverer{
		MinLevel: 10,
		MaxLevel: 10,
		MaxCells: 10,
		LevelMod: 0,
	}

	return &DynGeo{
		Config: config,
		db:     newDB(config),
	}, nil
}

func (dg DynGeo) PutPoint(input PutPointInput) (*PutPointOutput, error) {
	return dg.db.putPoint(nil, input)
}

func (dg DynGeo) PutPointWithContext(ctx context.Context, input PutPointInput) (*PutPointOutput, error) {
	return dg.db.putPoint(ctx, input)
}

func (dg DynGeo) BatchWritePoints(inputs []PutPointInput) (*BatchWritePointOutput, error) {
	return dg.db.batchWritePoints(nil, inputs)
}

func (dg DynGeo) BatchWritePointsWithContext(ctx context.Context, inputs []PutPointInput) (*BatchWritePointOutput, error) {
	return dg.db.batchWritePoints(ctx, inputs)
}

func (dg DynGeo) GetPoint(input GetPointInput) (*GetPointOutput, error) {
	return dg.db.getPoint(nil, input)
}

func (dg DynGeo) GetPointWithContext(ctx context.Context, input GetPointInput) (*GetPointOutput, error) {
	return dg.db.getPoint(ctx, input)
}

func (dg DynGeo) UpdatePoint(input UpdatePointInput) (*UpdatePointOutput, error) {
	return dg.db.updatePoint(nil, input)
}

func (dg DynGeo) UpdatePointWithContext(ctx context.Context, input UpdatePointInput) (*UpdatePointOutput, error) {
	return dg.db.updatePoint(ctx, input)
}

func (dg DynGeo) DeletePoint(input DeletePointInput) (*DeletePointOutput, error) {
	return dg.db.deletePoint(nil, input)
}

func (dg DynGeo) DeletePointWithContext(ctx context.Context, input DeletePointInput) (*DeletePointOutput, error) {
	return dg.db.deletePoint(ctx, input)
}

func (dg DynGeo) QueryRadius(input QueryRadiusInput, out interface{}) error {
	output, err := dg.queryRadius(nil, input)
	if err != nil {
		return err
	}

	return dg.unmarshallOutput(output, out)
}

func (dg DynGeo) QueryRadiusWithContext(ctx context.Context, input QueryRadiusInput, out interface{}) error {
	output, err := dg.queryRadius(ctx, input)
	if err != nil {
		return err
	}

	return dg.unmarshallOutput(output, out)
}

func (dg DynGeo) QueryRectangle(input QueryRectangleInput, out interface{}) error {
	output, err := dg.queryRectangle(nil, input)
	if err != nil {
		return err
	}

	return dg.unmarshallOutput(output, out)
}

func (dg DynGeo) QueryRectangleWithContext(ctx context.Context, input QueryRectangleInput, out interface{}) error {
	output, err := dg.queryRectangle(ctx, input)
	if err != nil {
		return err
	}

	return dg.unmarshallOutput(output, out)
}

func (dg DynGeo) queryRectangle(ctx context.Context, input QueryRectangleInput) ([]map[string]*dynamodb.AttributeValue, error) {
	latLngRect := rectFromQueryRectangleInput(input)
	covering := newCovering(dg.Config.s2RegionCoverer.Covering(s2.Region(latLngRect)))
	results := dg.dispatchQueries(ctx, covering, input.GeoQueryInput)

	return dg.filterByRect(results, input)
}

func (dg DynGeo) queryRadius(ctx context.Context, input QueryRadiusInput) ([]map[string]*dynamodb.AttributeValue, error) {
	latLngRect := boundingLatLngFromQueryRadiusInput(input)
	covering := newCovering(dg.Config.s2RegionCoverer.Covering(s2.Region(latLngRect)))
	results := dg.dispatchQueries(ctx, covering, input.GeoQueryInput)

	return dg.filterByRadius(results, input)
}

func (dg DynGeo) dispatchQueries(ctx context.Context, covering covering, input GeoQueryInput) []map[string]*dynamodb.AttributeValue {
	results := [][]*dynamodb.QueryOutput{}
	wg := &sync.WaitGroup{}
	mtx := &sync.Mutex{}

	hashRanges := covering.getGeoHashRanges(dg.Config.HashKeyLength)
	iterations := len(hashRanges)
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(i int) {
			defer wg.Done()
			g := hashRanges[i]
			hashKey := generateHashKey(g.rangeMin, dg.Config.HashKeyLength)
			output := dg.db.queryGeoHash(ctx, input.QueryInput, hashKey, g)
			mtx.Lock()
			results = append(results, output)
			mtx.Unlock()
		}(i)
	}

	wg.Wait()

	var mergedResults []map[string]*dynamodb.AttributeValue
	for _, o := range results {
		for _, r := range o {
			mergedResults = append(mergedResults, r.Items...)
		}
	}

	return mergedResults
}

func (dg DynGeo) filterByRect(list []map[string]*dynamodb.AttributeValue, input QueryRectangleInput) ([]map[string]*dynamodb.AttributeValue, error) {
	var filtered []map[string]*dynamodb.AttributeValue
	latLngRect := rectFromQueryRectangleInput(input)

	for _, item := range list {
		latLng, err := dg.latLngFromItem(item)
		if err != nil {
			return nil, err
		}

		if latLngRect.ContainsLatLng(*latLng) {
			filtered = append(filtered, item)
		}
	}

	return filtered, nil
}

func (dg DynGeo) filterByRadius(list []map[string]*dynamodb.AttributeValue, input QueryRadiusInput) ([]map[string]*dynamodb.AttributeValue, error) {
	var filtered []map[string]*dynamodb.AttributeValue

	centerLatLng := s2.LatLngFromDegrees(input.CenterPoint.Latitude, input.CenterPoint.Longitude)
	radius := input.RadiusInMeter

	for _, item := range list {
		latLng, err := dg.latLngFromItem(item)
		if err != nil {
			return nil, err
		}

		if getEarthDistance(centerLatLng, *latLng) <= float64(radius) {
			filtered = append(filtered, item)
		}
	}

	return filtered, nil
}

func (dg DynGeo) latLngFromItem(item map[string]*dynamodb.AttributeValue) (*s2.LatLng, error) {
	geoJSON := []byte(*item[dg.Config.GeoJSONAttributeName].S)
	geoJSONAttr := GeoJSONAttribute{}
	err := json.Unmarshal(geoJSON, &geoJSONAttr)
	if err != nil {
		return nil, err
	}

	coordinates := geoJSONAttr.Coordinates
	var lng float64
	var lat float64
	if dg.Config.LongitudeFirst {
		lng = coordinates[0]
		lat = coordinates[1]
	} else {
		lng = coordinates[1]
		lat = coordinates[0]
	}

	latLng := s2.LatLngFromDegrees(lat, lng)

	return &latLng, nil
}

func (dg DynGeo) unmarshallOutput(output []map[string]*dynamodb.AttributeValue, out interface{}) error {
	err := dynamodbattribute.UnmarshalListOfMaps(output, out)
	if err != nil {
		return err
	}

	return nil
}
