package metadatadb

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"go.formulabun.club/srb2kart/addons"
	"go.formulabun.club/storage"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const filesColl = "files"

var fileToIndex = strings.ToLower

type File struct {
	Filename string `json:`
	Checksum string `json:`
}

func (f File) ToKey() *url.URL {
	return storage.BaseURL.JoinPath(f.Filename, f.Checksum, f.Filename)
}

func (c *Client) AddFile(file File, ctx context.Context) (existed bool, err error) {
	col := c.getCollection(filesColl)
	t := addons.GetAddonType(file.Filename)
	res := col.FindOneAndReplace(
		ctx,
		bson.D{
			{"filename", file.Filename},
			{"checksum", file.Checksum},
		},
		bson.D{
			{"filename", file.Filename},
			{"checksum", file.Checksum},
			{"addonType", bson.D{
				{"kart", t&addons.KartFlag > 0},
				{"match", t&addons.MatchFlag > 0},
				{"race", t&addons.RaceFlag > 0},
				{"battle", t&addons.BattleFlag > 0},
				{"char", t&addons.CharFlag > 0},
				{"lua", t&addons.LuaFlag > 0},
			}},
		},
		options.FindOneAndReplace().SetUpsert(true),
	)

	if res.Err() == mongo.ErrNoDocuments {
		return false, nil
	}

	return true, res.Err()
}

// operation is either the string "and" or the string "or". It is used to search for
// the combination of the AddonType flags.
func (c *Client) FindFiles(t addons.AddonType, operation string, ctx context.Context) ([]File, error) {
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
		return []File{}, err
	}

	files := make([]File, 0)

	err = cursor.All(ctx, &files)
	if err != nil {
		return []File{}, err
	}

	return files, nil
}
