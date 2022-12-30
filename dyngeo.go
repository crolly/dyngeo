package dyngeo

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/golang/geo/s2"
	"github.com/imdario/mergo"
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

func (dg DynGeo) PutPoint(ctx context.Context, input PutPointInput) (*PutPointOutput, error) {
	return dg.db.putPoint(ctx, input)
}

func (dg DynGeo) BatchWritePoints(ctx context.Context, inputs []PutPointInput) (*BatchWritePointOutput, error) {
	return dg.db.batchWritePoints(ctx, inputs)
}

func (dg DynGeo) GetPoint(ctx context.Context, input GetPointInput) (*GetPointOutput, error) {
	return dg.db.getPoint(ctx, input)
}

func (dg DynGeo) UpdatePoint(ctx context.Context, input UpdatePointInput) (*UpdatePointOutput, error) {
	return dg.db.updatePoint(ctx, input)
}

func (dg DynGeo) DeletePoint(ctx context.Context, input DeletePointInput) (*DeletePointOutput, error) {
	return dg.db.deletePoint(ctx, input)
}

func (dg DynGeo) QueryRadius(ctx context.Context, input QueryRadiusInput, out interface{}) error {
	output, err := dg.queryRadius(ctx, input)
	if err != nil {
		return err
	}

	return dg.unmarshallOutput(output, out)
}

type GeoHashToLastEvaluatedDBValue map[uint64]map[string]types.AttributeValue

func (dg DynGeo) QueryRadiusPaginated(ctx context.Context, input QueryRadiusInput, hashToLastEvaluatedEntry GeoHashToLastEvaluatedDBValue, limit uint, out interface{}) (GeoHashToLastEvaluatedDBValue, error) {
	if limit == 0 {
		return nil, errors.New("invalid limit provided")
	}
	output, newHashToLEntry, err := dg.queryRadiusWithPaginatedParams(ctx, input, hashToLastEvaluatedEntry, limit)
	if err != nil {
		return nil, err
	}
	return newHashToLEntry, dg.unmarshallOutput(output, out)
}

func (dg DynGeo) queryRadiusWithPaginatedParams(ctx context.Context, input QueryRadiusInput, hashToLastEvaluatedEntry GeoHashToLastEvaluatedDBValue, limit uint) ([]map[string]types.AttributeValue, GeoHashToLastEvaluatedDBValue, error) {
	latLngRect := boundingLatLngFromQueryRadiusInput(input)
	covering := newCovering(dg.Config.s2RegionCoverer.Covering(s2.Region(latLngRect)))
	results, newHashToLEntry := dg.dispatchQueriesWithPagination(ctx, covering, input.GeoQueryInput, hashToLastEvaluatedEntry, limit)
	filteredEntries, err := dg.filterByRadius(results, input)
	return filteredEntries, newHashToLEntry, err
}

func (dg DynGeo) QueryRectangle(ctx context.Context, input QueryRectangleInput, out interface{}) error {
	output, err := dg.queryRectangle(ctx, input)
	if err != nil {
		return err
	}

	return dg.unmarshallOutput(output, out)
}

func (dg DynGeo) queryRectangle(ctx context.Context, input QueryRectangleInput) ([]map[string]types.AttributeValue, error) {
	latLngRect := rectFromQueryRectangleInput(input)
	covering := newCovering(dg.Config.s2RegionCoverer.Covering(s2.Region(latLngRect)))
	results := dg.dispatchQueries(ctx, covering, input.GeoQueryInput)

	return dg.filterByRect(results, input)
}

func (dg DynGeo) queryRadius(ctx context.Context, input QueryRadiusInput) ([]map[string]types.AttributeValue, error) {
	latLngRect := boundingLatLngFromQueryRadiusInput(input)
	covering := newCovering(dg.Config.s2RegionCoverer.Covering(s2.Region(latLngRect)))
	results := dg.dispatchQueries(ctx, covering, input.GeoQueryInput)

	return dg.filterByRadius(results, input)
}

func (dg DynGeo) dispatchQueriesWithPagination(ctx context.Context, covering covering, input GeoQueryInput, hashToLastEvaluatedEntry GeoHashToLastEvaluatedDBValue, limit uint) ([]map[string]types.AttributeValue, GeoHashToLastEvaluatedDBValue) {
	var results [][]*dynamodb.QueryOutput
	wg := &sync.WaitGroup{}
	mtx := &sync.Mutex{}

	hashRanges := covering.getGeoHashRanges(dg.Config.HashKeyLength)
	iterations := len(hashRanges)
	newLastEvaluatedEntries := make(GeoHashToLastEvaluatedDBValue)
	wg.Add(iterations)
	for i := 0; i < iterations; i++ {
		go func(i int, queryInput dynamodb.QueryInput) {
			defer wg.Done()
			g := hashRanges[i]
			hashKey := generateHashKey(g.rangeMin, dg.Config.HashKeyLength)
			// look into the map to check if there has been a query for this hash before
			// if there was - reuse the last evaluated key for the hash
			if hashToLastEvaluatedEntry != nil {
				if lastEvalKey, ok := hashToLastEvaluatedEntry[hashKey]; ok {
					// this will only be true when there is in fact no more entries to be processed
					if lastEvalKey == nil {
						return
					}
					queryInput.ExclusiveStartKey = lastEvalKey
				}
			}
			// query hash and stop if reach the limit
			output := dg.db.queryGeoHash(ctx, queryInput, hashKey, g, int(limit))
			if len(output) > 0 {
				mtx.Lock()
				newLastEvaluatedEntries[hashKey] = output[len(output)-1].LastEvaluatedKey
				results = append(results, output)
				mtx.Unlock()
			}
		}(i, input.QueryInput)
	}

	wg.Wait()

	var mergedResults []map[string]types.AttributeValue
	for _, o := range results {
		for _, r := range o {
			mergedResults = append(mergedResults, r.Items...)
		}
	}
	return mergedResults, newLastEvaluatedEntries
}

func (dg DynGeo) dispatchQueries(ctx context.Context, covering covering, input GeoQueryInput) []map[string]types.AttributeValue {
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
			output := dg.db.queryGeoHash(ctx, input.QueryInput, hashKey, g, -1)
			mtx.Lock()
			results = append(results, output)
			mtx.Unlock()
		}(i)
	}

	wg.Wait()

	var mergedResults []map[string]types.AttributeValue
	for _, o := range results {
		for _, r := range o {
			mergedResults = append(mergedResults, r.Items...)
		}
	}

	return mergedResults
}

func (dg DynGeo) filterByRect(list []map[string]types.AttributeValue, input QueryRectangleInput) ([]map[string]types.AttributeValue, error) {
	var filtered []map[string]types.AttributeValue
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

func (dg DynGeo) filterByRadius(list []map[string]types.AttributeValue, input QueryRadiusInput) ([]map[string]types.AttributeValue, error) {
	var filtered []map[string]types.AttributeValue

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

func (dg DynGeo) latLngFromItem(item map[string]types.AttributeValue) (*s2.LatLng, error) {
	switch geo := item[dg.Config.GeoJSONAttributeName].(type) {
	case *types.AttributeValueMemberB:
		geoJSONAttr := GeoJSONAttribute{}
		err := json.Unmarshal(geo.Value, &geoJSONAttr)
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
		log.Println(latLng.String())
		return &latLng, nil
	}

	return nil, errors.New("invalid item at " + dg.Config.GeoJSONAttributeName)
}

func (dg DynGeo) unmarshallOutput(output []map[string]types.AttributeValue, out interface{}) error {
	err := attributevalue.UnmarshalListOfMaps(output, out)
	if err != nil {
		return err
	}

	return nil
}
