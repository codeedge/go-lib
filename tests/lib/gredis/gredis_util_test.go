package gredis

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/codeedge/go-lib/lib/gredis"
	"github.com/codeedge/go-lib/lib/gredis/config"
	"github.com/codeedge/go-lib/lib/gredis/mode/alone"
	"github.com/gogf/guuid"

	jsoniter "github.com/json-iterator/go"

	"github.com/gogf/gf/util/gconv"

	"github.com/gomodule/redigo/redis"
)

func InitRedis() {
	//mode := sentinel.NewByConfig(
	//	"mymaster1",
	//	config.NewConfig2(
	//		"192.168.2.110:26379",
	//		0,
	//		"yw123456!@#",
	//		2000,
	//		10,
	//	))

	mode := alone.NewByConfig(
		config.NewConfig2(
			"125.65.44.87:34534",
			0,
			"testvideoredis",
			2000,
			10,
		))
	gredis.CreateRedisgo(mode)
}

func Test_gredis_util_base(t *testing.T) {
	InitRedis()
	prefix := "testRedisGo:"
	key := prefix + "Get"
	gredis.GetRedis().Set(key, "testValue")
	v, _ := gredis.GetRedis().Get(key)
	fmt.Println("GET ===>>>key=", key, ", value=", v)
	fmt.Println()
	gredis.GetRedis().Set(key, "testValue11")
	delV0, _ := gredis.GetRedis().Get(key)
	gredis.GetRedis().Del(key)
	delV, _ := gredis.GetRedis().Get(key)
	fmt.Println("GET ===>>>key=", key, ", delBeforeValue=", delV0, ", value=", delV)

	key = prefix + "SetArgs"
	uuid := guuid.New().String()
	res, err1 := gredis.GetRedis().SetArgs(key, uuid, "NX", "EX", 3600*24)
	fmt.Printf("res=%s,err=%v \n", res, err1)
	res, err1 = gredis.GetRedis().SetArgs(key, uuid, "NX", "EX", 3600*24)
	fmt.Printf("res=%s,err=%v \n", res, err1)

	key = prefix + "SetArgs2"
	res, err1 = gredis.GetRedis().SetArgs(key, "test", "EX", 3600*24)
	fmt.Printf("res=%s,err=%v \n", res, err1)
	res, err1 = gredis.GetRedis().SetArgs(key, "test", "EX", 3600*24)
	fmt.Printf("res=%s,err=%v \n", res, err1)

	key = prefix + "Incr"
	_v1, _ := gredis.GetRedis().Get(key)
	v1, _ := gredis.GetRedis().Incr(key)
	fmt.Println("Incr ===>>>key=", key, ", beforeValue=", _v1, ", value=", v1)

	key = prefix + "IncrBy"
	_v2, _ := gredis.GetRedis().Get(key)
	v2, _ := gredis.GetRedis().IncrBy(key, 10)
	fmt.Println("IncrBy ===>>>key=", key, ", beforeValue=", _v2, ", value=", v2)
	fmt.Println()

	key = prefix + "Setex"
	gredis.GetRedis().Setex(key, 10, 456)
	v3, _ := gredis.GetRedis().Get(key)
	fmt.Println("Setex ===>>>key=", key, ", value=", v3)
	fmt.Println()

	key = prefix + "Zadd"
	gredis.GetRedis().Zadd(key, 10, "member1")
	gredis.GetRedis().Zadd(key, 10, "member2")

	gredis.GetRedis().Zincrby(key, 10, "member1")

	dataMap1, _ := gredis.GetRedis().ZrandgeWithScores(key, 0, -1)
	fmt.Printf("ZrandgeWithScores ===>>>key=%s, value=%v\n", key, dataMap1)

	dataStrs1, _ := gredis.GetRedis().Zrange(key, 0, -1)
	fmt.Printf("Zrange ===>>>key=%s, value=%v\n", key, dataStrs1)

	dataStrs2, _ := gredis.GetRedis().Zrank(key, "member1")
	fmt.Printf("Zrank ===>>>key=%s, value=%v\n", key, dataStrs2)

	strings, _ := gredis.GetRedis().ZrangeByScore(key, 0, 1000)
	fmt.Printf("ZrangeByScore ===>>>key=%s, value=%v\n", key, strings)

	mapStrings, _ := gredis.GetRedis().ZrangeByScoreWithScores(key, 0, 1000)
	fmt.Printf("ZrangeByScoreWithScores ===>>>key=%s, value=%v\n", key, mapStrings)
	fmt.Println()

	key = prefix + "Hset"
	_, err := gredis.GetRedis().Hset(key, "key1", "value1")
	fmt.Println(err)
	v4, _ := gredis.GetRedis().Hget(key, "key1")
	fmt.Printf("Hset ===>>>key=%s, value=%v\n", key, v4)

	gredis.GetRedis().Hset(key, "key2", "1")
	i, err := gredis.GetRedis().Hincrby(key, "key2", 10)
	fmt.Println(i, err)
	v5, _ := gredis.GetRedis().Hget(key, "key2")
	fmt.Printf("Hincrby ===>>>key=%s, value=%v\n", key, v5)
	fmt.Println()

	//t.Run("Hdel", func(t *testing.T) {
	//		hdel, _ := gredis.GetRedis().Hdel(key)
	//		fmt.Printf("Hdel ===>>>key=%s, value=%v\n", key, hdel)
	//		fmt.Println()
	//})

	key = prefix + "Lpush"
	//v6, _ := gredis.GetRedis().Lpush(key, "1", []string{"2", "_2"}, "3", "4", "5")
	v6, _ := gredis.GetRedis().Lpush(key, "1", "2", "3", "4", "5")
	fmt.Printf("Lpush ===>>>key=%s, value=%v\n", key, v6)

	key = prefix + "Rpush"
	v6, _ = gredis.GetRedis().Rpush(key, "1", "2", "3", "4", "5")
	fmt.Printf("Rpush ===>>>key=%s, value=%v\n", key, v6)

	key = prefix + "Lpop"
	v7, _ := gredis.GetRedis().Lpop(key)
	fmt.Printf("Lpop ===>>>key=%s, value=%v\n", key, v7)

	key = prefix + "Llen"
	v8, _ := gredis.GetRedis().Llen(key)
	fmt.Printf("Llen ===>>>key=%s, value=%v\n", key, v8)

	key = prefix + "Lrange"
	v9, _ := gredis.GetRedis().Lrange(key, 0, 3)
	fmt.Printf("Lrange ===>>>key=%s, value=%v\n", key, v9)

	key = prefix + "bit"
	key = prefix + "bit:"
	for i := 0; i < 2; i++ {
		key1 := key + strconv.Itoa(i)
		e := gredis.GetRedis().SetBit(key1, 0, 1)
		fmt.Printf("SetBit ===>>>key=%s, err=%v\n", key1, e)
		e = gredis.GetRedis().SetBit(key1, 3, 1)
		fmt.Printf("SetBit ===>>>key=%s, err=%v\n", key1, e)
		v10, _ := gredis.GetRedis().GetBit(key1, 0)
		fmt.Printf("GetBit ===>>>key=%s, value=%v\n", key1, v10)
		v11, _ := gredis.GetRedis().BitCount(key1)
		fmt.Printf("BitCount ===>>>key=%s, value=%v\n", key1, v11)
	}
	e := gredis.GetRedis().SetBit(key+"1", 1, 1)
	fmt.Printf("SetBit ===>>>key=%s, err=%v\n", key+"1", e)
	e = gredis.GetRedis().BitOp("AND", "and-result", key+"0", key+"1")
	fmt.Printf("BitOp ===>>>key=%s, err=%v\n", "and-result", e)
	for i := 0; i < 4; i++ {
		v10, _ := gredis.GetRedis().GetBit("and-result", int64(i))
		fmt.Printf("GetBit ===>>>key=%s, value=%v\n", key, v10)
	}
	v11, _ := gredis.GetRedis().BitCount("and-result")
	fmt.Printf("BitCount ===>>>key=%s, value=%v\n", "and-result", v11)

	//pipeline演示1 Receive每次只能接收1个消息
	//GETBIT命令测试会报错 c.Receive()返回超时错误
	gredis.GetRedis().Exec(func(c redis.Conn) (res interface{}, err error) {
		c.Send("SET", prefix+"my_test", 1)
		c.Send("SET", prefix+"my_test2", 2)
		c.Send("SET", prefix+"my_test3", 3)
		c.Send("GET", prefix+"my_test")
		c.Send("GET", prefix+"my_test2")
		c.Send("GET", prefix+"my_test3")
		c.Flush()
		for i := 0; i < 6; i++ {
			r, _ := c.Receive()
			//fmt.Println(err)
			//set返回ok get返回具体值，每次只能获取1个返回
			if r != "OK" {
				fmt.Println(string(r.([]byte)))
			}
		}
		return nil, err
	})

	//pipeline演示2
	gredis.GetRedis().Exec(func(c redis.Conn) (res interface{}, err error) {
		var value1 string
		var value2 string
		var value3 string

		c.Send("MULTI")
		c.Send("Get", prefix+"my_test")
		c.Send("Get", prefix+"my_test2")
		c.Send("Get", prefix+"my_test3")
		r, err := redis.Values(c.Do("EXEC"))
		if err != nil {
			return nil, err
		}
		if _, err := redis.Scan(r, &value1, &value2, &value3); err != nil {
			return nil, err
		} else {
			fmt.Println(value1)
			fmt.Println(value2)
			fmt.Println(value3)
			return nil, nil
		}
	})

	gredis.GetRedis().Select(2)
	gredis.GetRedis().Select(0)
	gredis.GetRedis().Set("eval-key", "value")
	gredis.GetRedis().Eval(`if redis.call("get",KEYS[1]) == ARGV[1]
										then
											return redis.call("del",KEYS[1])
										else
											return 0
										end`, 1, "eval-key", "value")
	gredis.GetRedis().Set("eval-key", "value")
	gredis.GetRedis().CheckAndDel("eval-key", "value")
}

func Test_gredis_util_01(t *testing.T) {
	InitRedis()

	prefix := "testRedisGo:"
	key := prefix + "test01"

	_, _ = gredis.GetRedis().Lpush(key, "test1")
	_, _ = gredis.GetRedis().Lpush(key, "test2")
	_, _ = gredis.GetRedis().Lpush(key, "test3")
	_, _ = gredis.GetRedis().Lpush(key, "test4")
	err := gredis.GetRedis().Ltrim(key, 1, 0)
	if err != nil {
		fmt.Println("Test_gredis_util_01, ", err)
	}
}

func Test_gredis_util_02(t *testing.T) {
	InitRedis()

	prefix := "testRedisGo:"
	key := prefix + "Hset-123"
	gredis.GetRedis().Hset(key, "key1", "value1")
	v4, _ := gredis.GetRedis().Hget(key, "key1")
	fmt.Printf("Hset ===>>>key=%s, value=%v\n", key, v4)

	gredis.GetRedis().Hset(key, "key2", "1")
	i, err := gredis.GetRedis().Hincrby(key, "key2", 10)
	fmt.Println(i, err)
	//v5, _ := gredis.GetRedis().Hget(key, "key2")
	//fmt.Printf("Hincrby ===>>>key=%s, value=%v\n", key, v5)
	//fmt.Println()

	hdel, err := gredis.GetRedis().Hdel(key, "key1")
	fmt.Printf("Hdel ===>>>key=%s, hdel=%v, err=%v\n", key, hdel, err)
	fmt.Println()
}

func Test_gredis_util_03(t *testing.T) {
	InitRedis()
	prefix := "testRedisGo:"
	key := prefix + "Rpush:test"

	gredis.GetRedis().Del(key)
	for i := 0; i < 1000; i++ {
		gredis.GetRedis().Rpush(key, gconv.String(i))
	}
	count, _ := gredis.GetRedis().Llen(key)
	fmt.Printf("Rpush ===>>>key=%s, count=%v\n", key, count)

	for i := 0; i < 1000/50; i++ {
		values, _ := gredis.GetRedis().Lrange(key, 0, 49)
		gredis.GetRedis().Ltrim(key, 50, -1)
		_count, _ := gredis.GetRedis().Llen(key)

		fmt.Printf("index=%d, values=%v, count=%d\n", i, values, _count)
	}

}

func Test_gredis_util_04(t *testing.T) {
	InitRedis()
	prefix := "testRedisGo:"
	key := prefix + "Setnx-1"
	res, _ := gredis.GetRedis().Setnx(key, gconv.String("123"))
	fmt.Println(res)
	res, _ = gredis.GetRedis().Setnx(key, gconv.String("456"))
	fmt.Println(res)

	gredis.GetRedis().SetValueIfNoExistExecFunc(prefix+"SetValueIfNoExistExecFunc", 123, func() {
		fmt.Println("执行成功")
	})
	gredis.GetRedis().SetValueIfNoExistExecFunc(prefix+"SetValueIfNoExistExecFunc", 123, func() {
		fmt.Println("执行成功")
	})
}

func Test_gredis_util_05(t *testing.T) {
	InitRedis()
	likeKey := "vp-server:cacheMode:tc_system_dict:set:id:*"
	u, results := gredis.GetRedis().ScanDataAndExecFuc(0, likeKey, -1, func(arrays []string) {
		jsonStr, _ := jsoniter.MarshalToString(arrays)
		fmt.Println(len(arrays), jsonStr)
	})
	fmt.Println("end==>>>", u, results, len(results))

}
