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

func (c *Client) AddMap(fileName string, mapData MapData, ctx context.Context) error {
	col := c.getCollection(mapsColl)

	res := col.FindOneAndReplace(
		ctx,
		bson.D{
			{"fileName", fileName},
			{"value.mapID", mapData.MapID},
		},
		bson.D{
			{"fileName", fileName},
			{"value", mapData},
		},
		options.FindOneAndReplace().SetUpsert(true),
	)
	if res.Err() == mongo.ErrNoDocuments {
		return nil
	}
	return res.Err()
}
