package metadatadb

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const mapsColl = "maps"

type LevelType string

type MapKey struct {
	MapID     string `bson:"mapID"`
	LevelName string `bson:"levelName"`
	SubTitle  string `bson:"subTitle"`
}

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

type MapList []MapListElement
type MapListElement struct {
	MapID     string `bson:"mapID"`
	LevelName string `bson:"levelName"`
	Act       string `bson:"act"`
	SubTitle  string `bson:"subTitle"`
	ZoneTitle string `bson:"zoneTitle"`
	NoZone    bool   `bson:"noZone"`
	NumLaps   int    `bson:"numLaps"`
	Filename  string `bson:"filename"`
}

func (c *Client) FindMaps(inFile string, mapKey *MapKey, ctx context.Context) (MapList, error) {
	col := c.getCollection(mapsColl)
	pipeline := mongo.Pipeline{}

	// filter on given parameters
	matchFilters := bson.M{}
	if inFile != "" {
		matchFilters["filename"] = inFile
	}

	if mapKey != nil {
		matchFilters["mapdata.mapID"] = mapKey.MapID
		matchFilters["mapdata.levelName"] = mapKey.LevelName
		matchFilters["mapdata.subTitle"] = mapKey.SubTitle
	}

	pipeline = append(pipeline,
		bson.D{
			{"$match",
				matchFilters,
			},
		},
	)

	// group on mapID
	pipeline = append(pipeline,
		bson.D{
			{
				"$group",
				bson.M{
					"_id":       bson.A{"$mapdata.mapID", "$mapdata.levelName", "$mapdata.subTitle"},
					"mapID":     bson.M{"$first": "$mapdata.mapID"},
					"levelName": bson.M{"$first": "$mapdata.levelName"},
					"act":       bson.M{"$first": "$mapdata.act"},
					"subTitle":  bson.M{"$first": "$mapdata.subTitle"},
					"zoneTitle": bson.M{"$first": "$mapdata.zoneTitle"},
					"noZone":    bson.M{"$first": "$mapdata.noZone"},
					"numLaps":   bson.M{"$first": "$mapdata.numLaps"},
					"filename":  bson.M{"$first": "$filename"},
				},
			},
		},
	)

	// sort on mapID
	pipeline = append(pipeline,
		bson.D{
			{"$sort", bson.M{"mapID": 1}},
		},
	)

	cursor, err := col.Aggregate(ctx,
		pipeline,
	)

	if err != nil {
		return MapList{}, err
	}

	maps := make(MapList, 0)

	err = cursor.All(ctx, &maps)
	if err != nil {
		return MapList{}, err
	}

	return maps, nil
}
