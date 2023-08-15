package metadatadb

import (
	"context"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const metadataDB = "metadata"

type Client struct {
	*mongo.Client
}

func (c *Client) getCollection(collection string) *mongo.Collection {
	return c.Database(metadataDB).Collection(collection)
}

func NewClient(ctx context.Context) (*Client, error) {
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://metadata:27017"))
	if err != nil {
		return nil, err
	}
	return &Client{
		client,
	}, nil
}
