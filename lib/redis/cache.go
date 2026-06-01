package redis

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/mgtv-tech/jetcache-go"
	json2 "github.com/mgtv-tech/jetcache-go/encoding/json"
	"github.com/mgtv-tech/jetcache-go/local"
	"github.com/mgtv-tech/jetcache-go/remote"
)

var ErrRecordNotFound = errors.New("no data was found")

func (c *Client) initCache() {
	sourceID := uuid.New().String()
	channelName := "syncLocalChannel"
	pubSub := c.rdb.Subscribe(context.Background(), channelName)
	c.cache = cache.New(cache.WithName("any"),
		cache.WithRemote(remote.NewGoRedisV9Adapter(c.rdb)),
		cache.WithLocal(local.NewTinyLFU(10000, time.Minute)),
		cache.WithCodec(json2.Name),
		cache.WithErrNotFound(ErrRecordNotFound),
		cache.WithRefreshDuration(time.Minute),
		cache.WithStopRefreshAfterLastAccess(time.Hour),
		cache.WithSourceId(sourceID),
		cache.WithSyncLocal(true),
		cache.WithStatsDisabled(true),
		cache.WithEventHandler(func(event *cache.Event) {
			bs, _ := json.Marshal(event)
			c.rdb.Publish(context.Background(), channelName, string(bs))
		}),
	)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Println("setJetCache panic:", r)
			}
		}()
		for {
			msg := <-pubSub.Channel()
			var event *cache.Event
			if err := json.Unmarshal([]byte(msg.Payload), &event); err != nil {
				log.Println("Error unmarshalling message:", err)
				continue
			}

			if event.SourceID != sourceID {
				for _, key := range event.Keys {
					c.cache.DeleteFromLocalCache(key)
				}
			}
		}
	}()
}
