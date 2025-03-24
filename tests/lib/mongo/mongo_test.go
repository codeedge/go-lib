package mongoext

import (
	"context"
	"fmt"
	"testing"

	"github.com/gogf/gf/os/gtime"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/gogf/gf/frame/g"
	"github.com/kdcer/go-lib/lib/mongoext"
)

func Test_Mongo(t *testing.T) {
	mongoext.InitMongo(g.Config().GetString("mongo.uri"), g.Config().GetString("mongo.db"), g.Config().GetUint64("mongo.maxPoolSize"))
	InitMongoExt()
	res, err := MongoExtend.Banners.InsertOne(context.Background(), &Banners{
		BaseMongo: BaseMongo{
			ID:        primitive.NewObjectID(),
			CreatedAt: gtime.Now().Format("Y-m-d H:i:s"),
			UpdatedAt: gtime.Now().Format("Y-m-d H:i:s"),
			DeletedAt: "",
		},
		Image: "1",
		Url:   "2",
	})

	var banner Banners
	err = MongoExtend.Banners.FindOne(context.Background(), bson.D{
		{"_id", res.InsertedID},
	}).Decode(&banner)

	banners := make([]*Banners, 0)
	filter := bson.M{}
	//if info.Image != "" {
	//	filter["image"] = info.Image
	//}
	//if info.Url != "" {
	//	filter["url"] = info.Url
	//}
	findOptions := mongoext.GetFindOptions(int64(1), int64(10), nil)
	cursor, err := MongoExtend.Banners.Find(context.Background(), filter, findOptions)
	if err != nil {
		return
	}
	err = cursor.All(context.Background(), &banners)
	if err != nil {
		fmt.Println(err)
	}
	//defer cursor.Close(context.Background())
	//var tmp model.Banners
	//for cursor.Next(context.Background()) {
	//	err = cursor.Decode(&tmp)
	//	if err != nil {
	//		fmt.Println(err)
	//		continue
	//	}
	//	banners = append(banners, &tmp)
	//}
}

type BaseMongo struct {
	ID        primitive.ObjectID `json:"id" bson:"_id"`
	CreatedAt string             `json:"createdAt" bson:"createdAt"` // 创建时间
	UpdatedAt string             `json:"updatedAt" bson:"updatedAt"` // 更新时间
	DeletedAt string             `json:"deletedAt" bson:"deletedAt"` // 删除时间
}

type MongoBase interface {
	TableName() string
}

type MongoExt struct {
	Client  *mongo.Client
	Banners *mongo.Collection
}

var MongoExtend *MongoExt

func InitMongoExt() {
	MongoExtend = &MongoExt{
		Client:  mongoext.Client,
		Banners: mongoext.GetCollect(new(Banners).TableName()),
	}
}

func (*Banners) TableName() string {
	return "banners"
}

type Banners struct {
	BaseMongo `bson:",inline"`
	Image     string `json:"image" bson:"image"`
	Url       string `json:"url" bson:"url"`
}
