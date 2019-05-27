# DynG(e)o

Unofficial Go port of the [Geo Library for Amazon DynamoDB](https://github.com/amazon-archives/dynamodb-geo) using [geohash](https://en.wikipedia.org/wiki/Geohash) to easily create and query geospatial data.
The library takes care of managing the geohash indexes and storing item with latitude/longitude pairs.

## Install

Fetch the package with

```
go get github.com/crolly/dyngeo
```

And import it into your programs with

```go
import "github.com/crolly/dyngeo"
```

## Usage

### DynG(e)o Configuration

```go
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

	DynamoDBClient  *dynamodb.DynamoDB
}
```

Defines, how DynG(e)o manages the geospatial data, e.g. what the db attribute and index names are as well as setting the geohash key length.
The geohash key length will determine the size of the tiles the planet will be seperated into:

| Length | Tile Size             |
| ------ |-----------------------|
| 1      | 5,009.4km x 4,992.6km |
| 2      | 1,252.3km x 624.1km   |
| 3      | 156.5km x 156km       |
| 4      | 39.1km x 19.5km       |
| 5      | 4.9km x 4.9km         |
| 6      | 1.2km x 609.4m        |
| 7      | 152.9m x 152.4m       |
| 8      | 38.2m x 19m           |
| 9      | 4.8m x 4.8m           |
| 10     | 1.2m x 59.5cm         |
| 11     | 14.9cm x 14.9cm       |
| 12     | 3.7cm x 1.9cm         |

Setting `DynamoDBClient *dynamodb.DynamoDB` and `TableName string` is required.

### DynG(e)o Instance

#### func New

```go
func New(config DynGeoConfig) (*DynGeo, error)
```
Returns a new instance of `DynG(e)o` managing the geohashing and geospatial db operations.

#### func PutPoint

```go
func (dg DynGeo) PutPoint(input PutPointInput) (*PutPointOutput, error)
```
Put a point into the Amazon DynamoDB table. Once put, you cannot update attributes specified in GeoDataManagerConfiguration: hash key, range key, geohash and geoJson. If you want to update these columns, you need to insert a new record and delete the old record.

#### func BatchWritePoints

```go
func (dg DynGeo) BatchWritePoints(inputs []PutPointInput) (*BatchWritePointOutput, error)
```
Put a list of points into the Amazon DynamoDB table. Once put, you cannot update attributes specified in GeoDataManagerConfiguration: hash key, range key, geohash and geoJson. If you want to update these columns, you need to insert a new record and delete the old record.

#### func GetPoint

```go
func (dg DynGeo) GetPoint(input GetPointInput) (*GetPointOutput, error)
```
Get a point from the Amazon DynamoDB table.

#### func UpdatePoint

```go
func (dg DynGeo) UpdatePoint(input UpdatePointInput) (*UpdatePointOutput, error)
```
Update a point data in Amazon DynamoDB table. You cannot update attributes specified in GeoDataManagerConfiguration: hash key, range key, geohash and geoJson. If you want to update these columns, you need to insert a new record and delete the old record.

#### func DeletePoint

```go
func (dg DynGeo) DeletePoint(input DeletePointInput) (*DeletePointOutput, error)
```
Delete a point from the Amazon DynamoDB table.

#### func QueryRadius

```go
func (dg DynGeo) QueryRadius(input QueryRadiusInput, out interface{}) error 
```
Query a circular area constructed by a center point and its radius.

#### func  QueryRectangle

```go
func (dg DynGeo) QueryRectangle(input QueryRectangleInput, out interface{}) error 
```
Query a rectangular area constructed by two points and return all points within the area. Two points need to construct a rectangle from minimum and maximum latitudes and longitudes. If minPoint.Longitude > maxPoint.Longitude, the rectangle spans the 180 degree longitude line.

## Getting Started Example

This repository contains a Getting Started example in the folder `starbucks-example` inspired by James Beswick's very good blog post about [Location-based search results with DynamoDB and Geohash](https://read.acloud.guru/location-based-search-results-with-dynamodb-and-geohash-267727e5d54f)

It uses the US Starbucks locations, loads them into DynamoDB in batches of 25 and then retrieves the the locations of all Starbucks in the radius of 5000 meters surrounding Latitude:  40.7769099, Longitude: -73.9822532.

The example illustrates the general usage as well as a hint of the performance. The radius search constantly needs about 20-30ms with the given dataset (approximately 6500 Starbucks coffee shops in the US).
