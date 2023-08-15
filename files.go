package metadatadb

import (
	"context"
	"fmt"
	"strings"

	"go.formulabun.club/srb2kart/addons"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const filesColl = "files"

var fileToIndex = strings.ToLower

func (c *Client) AddFile(fileName string, ctx context.Context) error {
	col := c.getCollection(filesColl)
	t := addons.GetAddonType(fileName)
	res := col.FindOneAndReplace(
		ctx,
		bson.D{
			{"_id", fileToIndex(fileName)},
		},
		bson.D{
			{"_id", fileToIndex(fileName)},
			{"addonType", bson.D{
				{"kart", t&addons.KartFlag > 0},
				{"match", t&addons.MatchFlag > 0},
				{"race", t&addons.RaceFlag > 0},
				{"battle", t&addons.BattleFlag > 0},
				{"char", t&addons.CharFlag > 0},
				{"lua", t&addons.LuaFlag > 0},
			}},
			{"fileName", fileName},
		},
		options.FindOneAndReplace().SetUpsert(true),
	)
	if res.Err() == mongo.ErrNoDocuments {
		return nil
	}
	return res.Err()
}

// operation is either the string "and" or the string "or". It is used to search for
// the combination of the AddonType flags.
func (c *Client) FindFiles(t addons.AddonType, operation string, ctx context.Context) ([]string, error) {
	col := c.getCollection(filesColl)

	var andor string
	switch operation {
	case "and", "or":
		andor = fmt.Sprintf("$%s", operation)
	default:
		andor = "$or"
	}

	filter := []bson.D{}
	if t&addons.KartFlag != 0 {
		filter = append(filter, bson.D{{"addonType.kart", true}})
	}
	if t&addons.MatchFlag != 0 {
		filter = append(filter, bson.D{{"addonType.match", true}})
	}
	if t&addons.RaceFlag != 0 {
		filter = append(filter, bson.D{{"addonType.race", true}})
	}
	if t&addons.BattleFlag != 0 {
		filter = append(filter, bson.D{{"addonType.battle", true}})
	}
	if t&addons.CharFlag != 0 {
		filter = append(filter, bson.D{{"addonType.char", true}})
	}
	if t&addons.LuaFlag != 0 {
		filter = append(filter, bson.D{{"addonType.lua", true}})
	}
	cursor, err := col.Find(
		ctx,
		bson.D{{andor, filter}},
		options.Find().SetSort(
			bson.D{
				{"_id", 1},
			},
		),
	)

	if err != nil {
		return []string{}, err
	}

	files := make([]struct {
		FileName string
	}, 0)

	err = cursor.All(ctx, &files)
	if err != nil {
		return []string{}, err
	}

	result := make([]string, len(files))
	for i := range result {
		result[i] = files[i].FileName
	}

	return result, nil
}
