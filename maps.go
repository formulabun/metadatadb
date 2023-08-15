package metadatadb

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const mapsColl = "maps"

type LevelType string

type MapData struct {
	MapID       string    `bson:"mapID"`
	LevelName   string    `bson:"levelName"`
	Act         string    `bson:"act"`
	SubTitle    string    `bson:"subTitle"`
	ZoneTitle   string    `bson:"zoneTitle"`
	NoZone      bool      `bson:"noZone"`
	TypeOfLevel LevelType `bson:"typeOfLevel"`
	Palette     int       `bson:"palette"`
	Sky         int       `bson:"sky"`
	NumLaps     int       `bson:"numLaps"`
	Music       string    `bson:"music"`
}

type StoredMapData struct {
	MapData
	Filename string `bson:"filename"`
}

func (c *Client) AddMap(fileName string, mapData MapData, ctx context.Context) error {
	col := c.getCollection(mapsColl)

	data := StoredMapData{mapData, fileName}
	res := col.FindOneAndReplace(
		ctx,
		bson.D{
			{"filename", fileName},
			{"mapdata.mapID", mapData.MapID},
		},
		data,
		options.FindOneAndReplace().SetUpsert(true),
	)
	if res.Err() == mongo.ErrNoDocuments {
		return nil
	}
	return res.Err()
}

func (c *Client) FindMaps(inFile string, ctx context.Context) ([]StoredMapData, error) {
	col := c.getCollection(mapsColl)

	filter := bson.D{}
	if inFile != "" {
		filter = append(filter, bson.E{"filename", inFile})
	}

	cursor, err := col.Find(ctx,
		filter,
		options.Find().SetSort(bson.D{{"mapdata.mapID", 1}}),
	)

	if err != nil {
		return []StoredMapData{}, err
	}

	maps := make([]StoredMapData, 0)

	err = cursor.All(ctx, &maps)
	if err != nil {
		return []StoredMapData{}, err
	}

	return maps, nil
}
