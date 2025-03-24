package mongoext

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var syncOnce sync.Once
var Client *mongo.Client
var Db *mongo.Database

func InitMongo(uri, db string, maxPoolSize uint64) {
	syncOnce.Do(
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri).SetMaxPoolSize(maxPoolSize))
			if err != nil {
				panic(err)
			}
			Client = _client
			Db = _client.Database(db)
		})
}

func GetCollect(tableName string) *mongo.Collection {
	return Db.Collection(tableName)
}

// GetFindOptions .
func GetFindOptions(page, pageSize int64, sort interface{}) *options.FindOptions {
	findOptions := options.Find()
	if sort != nil {
		findOptions.SetSort(sort)
	}
	if pageSize == 0 {
		pageSize = 20
	}
	findOptions.Limit = &pageSize
	skip := (page - 1) * pageSize
	findOptions.Skip = &skip
	return findOptions
}
