package metadatadb

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/biter777/countries"
	"github.com/phuslu/iploc"
	bunStrings "go.formulabun.club/functional/strings"
	"go.formulabun.club/srb2kart/network"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const serversColl = "servers"

type Server struct {
	Name       string
	Location   string
	Region     string
	HttpSource string
	File       []File
}

func (c *Client) AddServerFiles(host string, files []File, ctx context.Context) error {
	col := c.getCollection(serversColl)
	res := col.FindOneAndUpdate(
		ctx,
		bson.D{
			{"_id", host},
		},
		bson.D{
			{"$set", bson.D{
				{"_id", host},
				{"host", host},
				{"files", files},
			}},
		},
		options.FindOneAndUpdate().SetUpsert(true),
	)

	if res.Err() == mongo.ErrNoDocuments {
		return nil
	}
	return res.Err()
}

func (c *Client) AddServerInfo(host string, info network.ServerInfo, ctx context.Context) error {
	col := c.getCollection(serversColl)
	countryCode, err := ParseHost(host)
	if err != nil {
		return err
	}
	res := col.FindOneAndUpdate(
		ctx,
		bson.D{
			{"_id", host},
		},
		bson.D{
			{"$set", bson.D{
				{"name", bunStrings.SafeNullTerminated((info.ServerName[:]))},
				{"httpSource", bunStrings.SafeNullTerminated(info.HttpSource[:])},
				{"location", countryCode.Alpha2()},
				{"region", countryCode.Region().String()},
			}},
		},
		options.FindOneAndUpdate().SetUpsert(true),
	)
	if res.Err() == mongo.ErrNoDocuments {
		return nil
	}
	return res.Err()
}

func (c *Client) GetServersWithFile(file, clientCountry, clientRegion string, ctx context.Context) ([]Server, error) {
	col := c.getCollection(serversColl)

	filter := bson.M{}
	filter["files"] = file
	if clientCountry != "" {
		filter["location"] = clientCountry
	}
	if clientRegion != "" {
		filter["location"] = clientRegion
	}

	cursor, err := col.Find(
		ctx,
		filter,
	)
	if err != nil {
		return []Server{}, err
	}

	res := []Server{}
	err = cursor.All(ctx, &res)
	return res, err
}

func ParseHost(host string) (countries.CountryCode, error) {
	hostFormatError := errors.New(fmt.Sprintf("Bad host format: '%s'", host))
	parts := strings.Split(host, ":")
	if len(parts) != 2 {
		return countries.Unknown, hostFormatError
	}
	ip := net.ParseIP(parts[0])
	if ip == nil {
		return countries.Unknown, hostFormatError
	}
	country := string(iploc.Country(ip))
	c := countries.ByName(country)
	if c == countries.Unknown {
		return countries.Unknown, nil
	}
	return c, nil
}
