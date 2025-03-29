package gredis

import (
	"errors"
	"sync"

	"github.com/codeedge/go-lib/lib/gredis/mode"

	"github.com/gogf/gf/os/glog"
	"github.com/gomodule/redigo/redis"
)

const (
	DefaultRedisName string = "default" //默认redis
)

var redisMap = map[string]*Redigo{}
var lock sync.Mutex

// 创建默认redis
func CreateRedisgo(mode mode.IMode) *Redigo {
	return CreateRedisGoByRedisName(DefaultRedisName, mode)
}

// 创建指定别名redis
func CreateRedisGoByRedisName(redisName string, mode mode.IMode) *Redigo {
	//lock.Lock()
	//defer lock.Unlock()
	//redisgo := New(mode)
	//if _, ok := redisMap[redisName]; ok {
	//	glog.Warning("已存在 redisName=", redisName)
	//	return redisMap[redisName]
	//}
	//redisMap[redisName] = redisgo
	//
	//ok, err := redisgo.IsConnectSuccess()
	//if ok { //检查redis配置,连接失败会panic
	//	glog.Infof("初始化成功 redisgo name=%s, ", redisName)
	//} else {
	//	panic("连接redis失败, 请检查配置 \n" + err.Error())
	//}
	//return redisgo

	lock.Lock()
	defer lock.Unlock()

	redisgo := redisMap[redisName]
	if redisgo != nil {
		glog.Warning("已存在 redisName=", redisName)
		return redisMap[redisName]
	} else {
		redisgo = CreateRedisGo(mode)
		redisMap[redisName] = redisgo
		glog.Infof("初始化成功并添加到redisMap中 redisgo name=%s, ", redisName)
	}
	return redisgo
}

// 创建RedisGo
func CreateRedisGo(mode mode.IMode) *Redigo {
	redisgo := New(mode)
	ok, err := redisgo.IsConnectSuccess()
	if ok { //检查redis配置,连接失败会panic
		glog.Infof("初始化成功 redisgo")
	} else {
		panic("连接redis失败, 请检查配置 \n" + err.Error())
	}
	return redisgo
}

// 获取默认redis
func GetRedis() *Redigo {
	if v, ok := redisMap[DefaultRedisName]; ok {
		return v
	}
	panic("不存在对应 redisgo redisName=" + DefaultRedisName)
}

// 获取指定name redis
func GetRedisByName(redisName string) *Redigo {
	if v, ok := redisMap[redisName]; ok {
		return v
	}
	panic("不存在对应 redisgo redisName=" + redisName)
}

func ClosePointRedisPool(redisName string) {
	redisGo := GetRedisByName(redisName)
	if redisGo != nil {
		delete(redisMap, redisName)
		defer redisGo.GetMode().GetPool().Close()
	}
}

//==============================================================================================
//==============================================================================================

func (r *Redigo) Exists(key string) (bool, error) {
	res, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("EXISTS", key)
	})
	return res > 0, e
}

// 为给定 key 设置生存时间(s)，当 key 过期时(生存时间为 0 )，它会被自动删除。
func (r *Redigo) Expire(key string, timeoutSec int64) (int, error) {
	v, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("EXPIRE", key, timeoutSec)
	})
	return v, e
}

// 查找所有符合给定模式 pattern 的 key
func (r *Redigo) Keys(key string) ([]string, error) {
	strs, e := r.Strings(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("KEYS", key)
	})
	return strs, e
}

// 全部转为string处理
func (r *Redigo) Get(key string) (string, error) {
	return r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("GET", key)
	})
}

func (r *Redigo) Set(key string, value interface{}) error {
	_, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SET", key, value)
	})
	return e
}

// 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)。
func (r *Redigo) TTL(key string) (int64, error) {
	return r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("TTL", key)
	})
}

// 直接传递扩展参数的Set
// 在 Redis 2.6.12 版本以前， SET 命令总是返回 OK 。
// 从 Redis 2.6.12 版本开始， SET 在设置操作成功完成时，才返回 OK 。
// 如果设置了 NX 或者 XX ，但因为条件没达到而造成设置操作未执行，那么命令返回空批量回复（NULL Bulk Reply）。
// EX 后面不可以跟0，会报错 (error) ERR invalid expire time in set
// 成功返回res=OK,err=<nil>
// NX或者XX失败返回res=,err=redigo: nil returned
func (r *Redigo) SetArgs(key string, args ...interface{}) (string, error) {
	res, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		realArgs := make([]interface{}, len(args)+1)
		realArgs[0] = key
		for i, value := range args {
			realArgs[i+1] = value
		}
		return c.Do("SET", realArgs...)
	})
	return res, e
}

func (r *Redigo) Del(key string) (int, error) {
	res, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("DEL", key)
	})
	return res, e
}

func (r *Redigo) Dels(keys ...string) (int, error) {
	res, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(keys))
		for i, value := range keys {
			args[i] = value
		}
		return c.Do("DEL", args...)
	})
	return res, e
}

// SCAN 命令是一个基于游标的迭代器（cursor based iterator）： SCAN 命令每次被调用之后， 都会向用户返回一个新的游标， 用户在下次迭代时需要使用这个新游标作为 SCAN 命令的游标参数， 以此来延续之前的迭代过程。
// 当 SCAN 命令的游标参数被设置为 0 时， 服务器将开始一次新的迭代， 而当服务器向用户返回值为 0 的游标时， 表示迭代已结束。
// 返回一个包含两个元素的
//
//	a.第一个元素是字符串表示的无符号 64 位整数（游标）
//	b.第二个元素包含了本次被迭代的元素。
func (r *Redigo) Scan(cursor uint64, likeKey string, count int) (uint64, []string, error) {
	i, e := r.Values(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, 0)
		args = append(args, cursor)
		args = append(args, "MATCH", likeKey)
		args = append(args, "COUNT", count)

		return c.Do("SCAN", args...)
	})

	if 2 != len(i) {
		return 0, nil, errors.New("gredis_util.Scan, 返回值异常")
	}

	nextCursor, e := redis.Uint64(i[0], e) //
	keys, e := redis.Strings(i[1], e)

	return nextCursor, keys, e
}

// 将 key 中储存的数字值增一。
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
func (r *Redigo) Incr(key string) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("INCR", key)
	})
	return v, e
}

// 将 key 所储存的值加上增量 increment 。
// 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY 命令。
// 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。
// 本操作的值限制在 64 位(bit)有符号数字表示之内。
func (r *Redigo) IncrBy(key string, value interface{}) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("INCRBY", key, value)
	})
	return v, e
}

// 指定的 key 设置值及其过期时间(秒)。
// 如果 key 已经存在， SETEX 命令将会替换旧的值。
func (r *Redigo) Setex(key string, timeoutSec int64, value interface{}) (string, error) {
	v, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SETEX", key, timeoutSec, value)
	})
	return v, e
}

// 将 key 的值设为 value ，当且仅当 key 不存在
// 若给定的 key 已经存在，则 SETNX 不做任何动作
// SETNX 是『SET if Not eXists』(如果不存在，则 SET)的简写
// 返回值：
// 设置成功，返回 1
// 设置失败，返回 0
func (r *Redigo) Setnx(key string, value interface{}) (int, error) {
	v, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SETNX", key, value)
	})
	return v, e
}

// SETNX 是『SET if Not eXists』(如果不存在，则 SET)的简写
// timeoutMilSec 过期时间(单位:毫秒)
func (r *Redigo) SetnxWithTimeout(key string, value interface{}, timeoutMilSec int64) (int, error) {
	v, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SET", key, value, "NX", "PX", timeoutMilSec)
	})
	return v, e
}

// 向有序结合添加（更新）一个或多个成员
func (r *Redigo) Zadd(key string, score float64, member string, scoreMember ...interface{}) error {
	_, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, 3)
		args[0] = key
		args[1] = score
		args[2] = member
		args = append(args, scoreMember...)
		return c.Do("ZADD", args...)
	})
	return e
}

// 为有序集 key 的成员 member 的 score 值加上增量 increment 。
// 可以通过传递一个负数值 increment ，让 score 减去相应的值，比如 ZINCRBY key -5 member ，就是让 member 的 score 值减去 5 。
// 当 key 不存在，或 member 不是 key 的成员时， ZINCRBY key increment member 等同于 ZADD key increment member 。
// 当 key 不是有序集类型时，返回一个错误。
// score 值可以是整数值或双精度浮点数。
func (r *Redigo) Zincrby(key string, increment float64, member string) error {
	_, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZINCRBY", key, increment, member)
	})
	return e
}

// 返回有序集 key 中，指定区间内的成员。
// 其中成员的位置按 score 值递增(从小到大)来排序。
// 具有相同 score 值的成员按字典序(lexicographical order )来排列。
// 如果你需要成员按 score 值递减(从大到小)来排列，请使用 ZREVRANGE 命令。
// 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
// 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
// 超出范围的下标并不会引起错误。
// 比如说，当 start 的值比有序集的最大下标还要大，或是 start > stop 时， ZRANGE 命令只是简单地返回一个空列表。
// 另一方面，假如 stop 参数的值比有序集的最大下标还要大，那么 Redis 将 stop 当作最大下标来处理。
// 可以通过使用 WITHSCORES 选项，来让成员和它的 score 值一并返回，返回列表以 value1,score1, ..., valueN,scoreN 的格式表示。
// 客户端库可能会返回一些更复杂的数据类型，比如数组、元组等。
func (r *Redigo) Zrange(key string, start int64, stop int64) ([]string, error) {
	v, e := r.Strings(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZRANGE", key, start, stop)
	})
	return v, e

}
func (r *Redigo) ZrandgeWithScores(key string, start int64, stop int64) (map[string]string, error) {
	v, e := r.StringMap(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZRANGE", key, start, stop, "WITHSCORES")
	})
	return v, e

}

// 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score 值递增(从小到大)次序排列。
// 具有相同 score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
// 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
// 可选的 WITHSCORES 参数决定结果集是单单返回有序集的成员，还是将有序集成员及其 score 值一起返回。
// 该选项自 Redis 2.0 版本起可用。
// 区间及无限
// min 和 max 可以是 -inf 和 +inf ，这样一来，你就可以在不知道有序集的最低和最高 score 值的情况下，使用 ZRANGEBYSCORE 这类命令。
// 默认情况下，区间的取值使用闭区间 (小于等于或大于等于)，你也可以通过给参数前增加 ( 符号来使用可选的开区间 (小于或大于)。
// 例如: 返回所有符合条件 5 < score < 10 的成员。 ==>> ZRANGEBYSCORE zset (5 (10
func (r *Redigo) ZrangeByScore(key string, start, stop float64) ([]string, error) {
	v, e := r.Strings(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZRANGEBYSCORE", key, start, stop)
	})
	return v, e
}
func (r *Redigo) ZrangeByScoreWithScores(key string, start, stop float64) (map[string]string, error) {
	v, e := r.StringMap(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZRANGEBYSCORE", key, start, stop, "WITHSCORES")
	})
	return v, e
}

// 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。
// 排名以 0 为底，也就是说， score 值最小的成员排名为 0 。
// 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil 。
// 时间复杂度: O(log(N))
func (r *Redigo) Zrank(key string, value string) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZRANK", key, value)
	})
	return v, e
}

// 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。
// 当 key 存在但不是有序集类型时，返回一个错误。
// 返回值: 被成功移除的成员的数量，不包括被忽略的成员。
func (r *Redigo) Zrem(key string, members ...string) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(members)+1)
		args[0] = key
		for i, value := range members {
			args[i+1] = value
		}
		return c.Do("ZREM", args...)
	})
	return v, e
}

// 移除有序集 key 中，指定排名(rank)区间内的所有成员。
// 区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内。
// 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
// 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。
// 返回值:被移除成员的数量。
func (r *Redigo) ZremRangeByRank(key string, start, stop int64) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZREMRANGEBYRANK", key, start, stop)
	})
	return v, e
}

// 移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
// 返回值:被移除成员的数量。
func (r *Redigo) ZremRangeByScore(key string, minScore float64, maxScore float64) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("ZREMRANGEBYSCORE", key, minScore, maxScore)
	})
	return v, e
}

func (r *Redigo) Hset(key string, field string, value string) (int, error) {
	v, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("HSET", key, field, value)
	})
	return v, e
}

func (r *Redigo) Hget(key string, field string) (string, error) {
	v, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("HGET", key, field)
	})
	return v, e
}

// 返回哈希表 key 中，所有的域和值。
// 在返回值里，紧跟每个域名(field name)之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
// 可用版本：
//
//	>= 2.0.0
//
// 时间复杂度：
//
//	O(N)， N 为哈希表的大小。
//
// 返回值：
//
//	以列表形式返回哈希表的域和域的值。
//	若 key 不存在，返回空列表。
func (r *Redigo) HgetAll(key string) (map[string]string, error) {
	strings, e := r.StringMap(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("HGETALL", key)
	})
	return strings, e
}

// 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
// 在Redis2.4以下的版本里， HDEL 每次只能删除单个域，如果你需要在一个原子时间内删除多个域，请将命令包含在 MULTI / EXEC 块内。
// 可用版本：
// >= 2.0.0
// 时间复杂度:
//
//	O(N)， N 为要删除的域的数量。
//
// 返回值:
//
//	被成功移除的域的数量，不包括被忽略的域。
func (r *Redigo) Hdel(key string, fields ...string) (int, error) {
	i, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(fields)+1)
		args[0] = key
		for i, value := range fields {
			args[i+1] = value
		}
		//args := rebuildArgs(key, fields)
		reply, err := c.Do("HDEL", args...)
		glog.Infof("reply=%v, err=%v", reply, err)
		return
	})
	return i, e
}

// 为哈希表 key 中的域 field 的值加上增量 increment 。
// 增量也可以为负数，相当于对给定域进行减法操作。
// 如果 key 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。
// 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
// 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。
// 本操作的值被限制在 64 位(bit)有符号数字表示之内。
// 返回值：
//
//	执行加法操作之后 field 域的值。
func (r *Redigo) Hincrby(key string, field string, increment int64) (int64, error) {
	i, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("HINCRBY", key, field, increment)
	})
	return i, e
}

// 为哈希表 key 中的域 field 加上浮点数增量 increment 。
// 如果哈希表中没有域 field ，那么 HINCRBYFLOAT 会先将域 field 的值设为 0 ，然后再执行加法操作。
// 如果键 key 不存在，那么 HINCRBYFLOAT 会先创建一个哈希表，再创建域 field ，最后再执行加法操作。
// 当以下任意一个条件发生时，返回一个错误：
// 域 field 的值不是字符串类型(因为 redis 中的数字和浮点数都以字符串的形式保存，所以它们都属于字符串类型）
// 域 field 当前的值或给定的增量 increment 不能解释(parse)为双精度浮点数(double precision floating point number)
// HINCRBYFLOAT 命令的详细功能和 INCRBYFLOAT 命令类似，请查看 INCRBYFLOAT 命令获取更多相关信息。
// 返回值：
//
//	执行加法操作之后 field 域的值。
func (r *Redigo) HincrbyFloat(key string, field string, increment float64) (float64, error) {
	i, e := r.Float64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("HINCRBYFLOAT", key, field, increment)
	})
	return i, e
}

// 查看哈希表 key 中，给定域 field 是否存在。
// 可用版本：
// >= 2.0.0
// 时间复杂度：
// O(1)
// 返回值：
// 如果哈希表含有给定域，返回 1 。
// 如果哈希表不含有给定域，或 key 不存在，返回 0 。
func (r *Redigo) Hexists(key string, field string) (bool, error) {
	b, e := r.Bool(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("HEXISTS", key, field)
	})
	return b, e
}

// HSCAN key cursor [MATCH pattern] [COUNT count]
func (r *Redigo) Hscan(key string, cursor uint64, likeKey string, count int) (uint64, map[string]string, error) {
	if count <= 0 {
		return 0, nil, errors.New("gredis_util.Hscan, count必须大于0")
	}

	i, e := r.Values(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, 0)
		args = append(args, key)
		args = append(args, cursor)
		if likeKey != "" {
			args = append(args, "MATCH", likeKey)
		}
		args = append(args, "COUNT", count)

		return c.Do("HSCAN", args...)
	})

	if 2 != len(i) {
		return 0, nil, errors.New("gredis_util.Hscan, 返回值异常")
	}

	nextCursor, e := redis.Uint64(i[0], e) //
	keys, e := redis.StringMap(i[1], e)

	return nextCursor, keys, e
}

// 将一个或多个值 value 插入到列表 key 的表头
// 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表头： 比如说，对空列表 mylist 执行命令 LPUSH mylist a b c ，列表的值将是 c b a ，这等同于原子性地执行 LPUSH mylist a 、 LPUSH mylist b 和 LPUSH mylist c 三个命令。
// 如果 key 不存在，一个空列表会被创建并执行 LPUSH 操作。
// 当 key 存在但不是列表类型时，返回一个错误。
func (r *Redigo) Lpush(key string, values ...interface{}) (int, error) {
	res, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(values)+1)
		args[0] = key
		for i, value := range values {
			args[i+1] = value
		}
		//args := rebuildArgs(key, values)
		return c.Do("LPUSH", args...)
	})
	return res, e
}

// 移除并返回列表 key 的头元素。
func (r *Redigo) Lpop(key string) (string, error) {
	v, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("LPOP", key)
	})
	return v, e
}

// 返回列表 key 的长度。
// 如果 key 不存在，则 key 被解释为一个空列表，返回 0 .
// 如果 key 不是列表类型，返回一个错误。
func (r *Redigo) Llen(key string) (int64, error) {
	v, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("LLEN", key)
	})
	return v, e
}

// 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定。
// 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
// 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
// 注意LRANGE命令和编程语言区间函数的区别
// 假如你有一个包含一百个元素的列表，对该列表执行 LRANGE list 0 10 ，结果是一个包含11个元素的列表，这表明 stop 下标也在 LRANGE 命令的取值范围之内(闭区间)，这和某些语言的区间函数可能不一致，比如Ruby的 Range.new 、 Array#slice 和Python的 range() 函数。
// 超出范围的下标
// 出范围的下标值不会引起错误超。
// 如果 start 下标比列表的最大下标 end ( LLEN list 减去 1 )还要大，那么 LRANGE 返回一个空列表。
// 如果 stop 下标比 end 下标还要大，Redis将 stop 的值设置为 end 。
func (r *Redigo) Lrange(key string, start, stop int) ([]string, error) {
	v, e := r.Strings(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("LRANGE", key, start, stop)
	})
	return v, e
}

// 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。
// 举个例子，执行命令 LTRIM list 0 2 ，表示只保留列表 list 的前三个元素，其余元素全部删除。
// 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
// 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。
// 当 key 不是列表类型时，返回一个错误。
// 命令执行成功时，返回 ok 。
func (r *Redigo) Ltrim(key string, start, stop int) error {
	_, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("LTRIM", key, start, stop)
	})
	return e
}

// 将一个或多个值 value 插入到列表 key 的表尾(最右边)。
// 如果有多个 value 值，那么各个 value 值按从左到右的顺序依次插入到表尾：比如对一个空列表 mylist 执行 RPUSH mylist a b c ，得出的结果列表为 a b c ，等同于执行命令 RPUSH mylist a 、 RPUSH mylist b 、 RPUSH mylist c 。
// 如果 key 不存在，一个空列表会被创建并执行 RPUSH 操作。
// 当 key 存在但不是列表类型时，返回一个错误。
func (r *Redigo) Rpush(key string, values ...interface{}) (int, error) {
	res, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(values)+1)
		args[0] = key
		for i, value := range values {
			args[i+1] = value
		}
		//args := rebuildArgs(key, values)
		return c.Do("RPUSH", args...)
	})
	return res, e
}

// 对 key 所储存的字符串值，获取指定偏移量上的位(bit)。
// 当 offset 比字符串值的长度大，或者 key 不存在时，返回 0 。
func (r *Redigo) GetBit(key string, offset int64) (i int64, err error) {
	i, err = r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("GETBIT", key, offset)
	})
	return
}

// 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。
// 位的设置或清除取决于 value 参数，可以是 0 也可以是 1 。
// 当 key 不存在时，自动生成一个新的字符串值。
// 字符串会进行伸展(grown)以确保它可以将 value 保存在指定的偏移量上。当字符串值进行伸展时，空白位置以 0 填充。
// offset 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在 512 MB 之内)。
func (r *Redigo) SetBit(key string, offset int64, bit int) error {
	_, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SETBIT", key, offset, bit)
	})
	return e
}

// 计算给定字符串中，被设置为 1 的比特位的数量。
// 一般情况下，给定的整个字符串都会被进行计数，通过指定额外的 start 或 end 参数，可以让计数只在特定的位上进行。
// start 和 end 参数的设置和 GETRANGE 命令类似，都可以使用负数值：比如 -1 表示最后一个位，而 -2 表示倒数第二个位，以此类推。
// 不存在的 key 被当成是空字符串来处理，因此对一个不存在的 key 进行 BITCOUNT 操作，结果为 0 。
// index代表start 和 end 不传默认对整个字符串计数
func (r *Redigo) BitCount(key string, index ...int) (i int64, err error) {
	i, err = r.Int64(func(c redis.Conn) (res interface{}, err error) {
		if len(index) == 2 {
			return c.Do("BITCOUNT", key, index[0], index[1])
		} else {
			return c.Do("BITCOUNT", key)
		}
	})
	return
}

// 对一个或多个保存二进制位的字符串 key 进行位元操作，并将结果保存到 destkey 上。
// operation 可以是 AND 、 OR 、 NOT 、 XOR 这四种操作中的任意一种：
// BITOP AND destkey key [key ...] ，对一个或多个 key 求逻辑并，并将结果保存到 destkey 。
// BITOP OR destkey key [key ...] ，对一个或多个 key 求逻辑或，并将结果保存到 destkey 。
// BITOP XOR destkey key [key ...] ，对一个或多个 key 求逻辑异或，并将结果保存到 destkey 。
// BITOP NOT destkey key ，对给定 key 求逻辑非，并将结果保存到 destkey 。
// 除了 NOT 操作之外，其他操作都可以接受一个或多个 key 作为输入。
func (r *Redigo) BitOp(operation, destkey string, keys ...interface{}) error {
	_, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		if len(keys) < 1 {
			return nil, errors.New("参数不合法")
		}
		args := []interface{}{operation, destkey}
		args = append(args, keys...)
		return c.Do("BITOP", args...)
	})
	return e
}

// 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。
// 假如 key 不存在，则创建一个只包含 member 元素作成员的集合。
// 当 key 不是集合类型时，返回一个错误。
// 返回值:
//
//	被添加到集合中的新元素的数量，不包括被忽略的元素。
func (r *Redigo) Sadd(key string, members ...string) (int, error) {
	i, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(members)+1)
		args[0] = key
		for i, value := range members {
			args[i+1] = value
		}
		return c.Do("SADD", args...)
	})
	return i, e
}

// 返回集合 key 中的所有成员。
// 不存在的 key 被视为空集合。
// 可用版本：
// >= 1.0.0
// 时间复杂度:
// O(N)， N 为集合的基数。
// 返回值:
// 集合中的所有成员。
func (r *Redigo) Smembers(key string) ([]string, error) {
	strings, e := r.Strings(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SMEMBERS", key)
	})
	return strings, e
}

// 移除并返回集合中的一个随机元素。
// 如果只想获取一个随机元素，但不想该元素从集合中被移除的话，可以使用 SRANDMEMBER 命令。
// 可用版本：
// >= 1.0.0
// 时间复杂度:
// O(1)
// 返回值:
// 被移除的随机元素。
// 当 key 不存在或 key 是空集时，返回 nil 。
func (r *Redigo) Spop(key string) (string, error) {
	res, e := r.String(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SPOP", key)
	})
	return res, e
}

// 判断 member 元素是否集合 key 的成员。
// 返回值:
// 如果 member 元素是集合的成员，返回 1 。
// 如果 member 元素不是集合的成员，或 key 不存在，返回 0 。
func (r *Redigo) Sismember(key string, member string) (int, error) {
	i, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SISMEMBER", key, member)
	})
	return i, e
}

// 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。
// 当 key 不是集合类型，返回一个错误。
// 在 Redis 2.4 版本以前， SREM 只接受单个 member 值。
// 可用版本：
// >= 1.0.0
// 时间复杂度:
// O(N)， N 为给定 member 元素的数量。
// 返回值:
// 被成功移除的元素的数量，不包括被忽略的元素。
func (r *Redigo) Srem(key string, members ...string) (int, error) {
	i, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		args := make([]interface{}, len(members)+1)
		args[0] = key
		for i, value := range members {
			args[i+1] = value
		}
		//args := rebuildArgs(key, members)
		return c.Do("SREM", args...)
	})
	return i, e
}

// 如果命令执行时，只提供了 key 参数，那么返回集合中的一个随机元素。
// 如果 count 为正数，且小于集合基数，那么命令返回一个包含 count 个元素的数组，数组中的元素各不相同。如果 count 大于等于集合基数，那么返回整个集合。
// 如果 count 为负数，那么命令返回一个数组，数组中的元素可能会重复出现多次，而数组的长度为 count 的绝对值。
func (r *Redigo) Srandmember(key string, count int) ([]string, error) {
	strings, e := r.Strings(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SRANDMEMBER", key, count)
	})

	return strings, e
}

//func rebuildArgs(key string, args []interface{}) (values []interface{}) {
//	values = make([]interface{}, len(args)+1)
//	values[0] = key
//	for i, value := range args {
//		values[i+1] = value
//	}
//	return values
//}

// select 切换库id
func (r *Redigo) Select(num int) error {
	_, e := r.Int64(func(c redis.Conn) (res interface{}, err error) {
		return c.Do("SELECT", num)
	})
	return e
}

// eval 执行lua脚本
func (r *Redigo) Eval(script string, args ...interface{}) (int, error) {
	res, e := r.Int(func(c redis.Conn) (res interface{}, err error) {
		realArgs := make([]interface{}, len(args)+1)
		realArgs[0] = script
		for i, value := range args {
			realArgs[i+1] = value
		}
		return c.Do("eval", realArgs...)
	})
	return res, e
}

// 检查value是key的值则删除，否则不操作
func (r *Redigo) CheckAndDel(key, value string) (int, error) {
	res, e := r.Eval(`if redis.call("get",KEYS[1]) == ARGV[1]
										then
											return redis.call("del",KEYS[1])
										else
											return 0
										end`, 1, key, value)
	return res, e
}
