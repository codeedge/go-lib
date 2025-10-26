package util

import (
	"bytes"
	"crypto/md5"
	crand "crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand"
	"slices"
	"sort"
	"time"

	"github.com/gogf/gf/v2/util/gconv"
)

// 生成32位md5字串
func Md5(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return hex.EncodeToString(h.Sum(nil))
}

// 生成uuid字串  使用此方法获取随机Guid
func UUID() string {
	b := make([]byte, 48)
	if _, err := io.ReadFull(crand.Reader, b); err != nil {
		return ""
	}
	return Md5(base64.URLEncoding.EncodeToString(b))
}

// 根据可变string拼接字符串
func NewBufferString(s ...string) string {
	l := len(s)
	if l == 0 {
		return ""
	}
	if l == 1 {
		return s[0]
	}
	b := bytes.NewBufferString(s[0])
	for i := 1; i < l; i++ {
		b.WriteString(s[i])
	}
	return b.String()
}

// String hashes a string to a unique hashcode.
// crc32 returns a uint32, but for our use we need
// and non negative integer. Here we cast to an integer
// and invert it if the result is negative.
// 并不能保证是绝对唯一的 比如：'49d3f976734b089fe0ba28960948459d' 和 'f0a8bbfdcf08874262abb3aeabdeaa69' 这两个字符串返回的结果是一样的都是687199550  只能用于不那么严格的场景
func HashCode(s string) int {
	v := int(crc32.ChecksumIEEE([]byte(s)))
	if v >= 0 {
		return v
	} else {
		return -v
	}
}

// 生成随机数据查询sql，先查询总数count，随机获取limit个不重复的offset(从0到count)，将这些随机数作为limit的偏移量 ，例如limit 10,1,limit 30,1。sql语句用union all 拼接。
// count 有效数据总数
// limit 查询的总数
// tableName 表名
// fields 字段
// conditions 条件
// order 排序
// return 生成的sql
func GenerateRandDataSql(count, limit int, tableName, fields, conditions, order string) string {
	offsets := GetRandIds(count, limit)
	sqlStr := bytes.NewBufferString("")
	lens := len(offsets)
	for i, v := range offsets {
		sqlStr.WriteString("(SELECT ")
		sqlStr.WriteString(fields)
		sqlStr.WriteString(" FROM ")
		sqlStr.WriteString(tableName)
		sqlStr.WriteString(" WHERE ")
		sqlStr.WriteString(conditions)
		sqlStr.WriteString(order)
		sqlStr.WriteString(" LIMIT " + gconv.String(v) + ",1) \n")
		if i < lens-1 {
			sqlStr.WriteString("union all \n")
		}
	}
	sqlStr.WriteString(";")
	return sqlStr.String()
}

// 随机查询数据，根据数据总量和查询数量返回随机数offset切片
// count 有效数据总数
// limit 查询的总数
// return 偏移量切片
func GetRandIds(count, limit int) []int {
	// 如果查询数量大于总数，只查询count个
	if limit > count {
		limit = count
	}
	rand.Seed(int64(HashCode(UUID()))) // 使用hashcode设置随机种子
	idMap := map[int]byte{}            // 用于判断是否已经存储
	offsets := []int{}                 // 用于存储的切片
	// 使用死循环，获取足够limit的随机数
	for {
		index := rand.Intn(count) // 从1到count-1
		// 如果没有存储才进行存储
		if _, ok := idMap[index]; !ok {
			idMap[index] = 0
			offsets = append(offsets, index)
		}
		// 获取足够的数据则返回
		if len(offsets) == limit {
			break
		}
	}
	return offsets
}

type Addable interface {
	int | int8 | int16 | int32 | int64 | uint | uint8 | uint16 | uint32 | uint64 | float32 | float64 | string
}

// RemoveRepByLoop 通过两重循环过滤重复元素  时间换空间
func RemoveRepByLoop[T Addable](slc []T) []T {
	result := make([]T, 0) // 存放结果
	for i := range slc {
		flag := true
		for j := range result {
			if slc[i] == result[j] {
				flag = false // 存在重复元素，标识为false
				break
			}
		}
		if flag { // 标识为false，不添加进结果
			result = append(result, slc[i])
		}
	}
	return result
}

// RemoveRepByMap 通过map主键唯一的特性过滤重复元素 空间换时间
func RemoveRepByMap[T Addable](slc []T) []T {
	slcLen := len(slc)
	result := make([]T, 0, slcLen)
	tempMap := make(map[T]struct{}, slcLen) // 存放不重复主键
	var l = 0
	for _, e := range slc {
		l = len(tempMap)
		tempMap[e] = struct{}{}
		if len(tempMap) != l { // 加入map后，map长度变化，则元素不重复
			result = append(result, e)
		}
	}
	return result
}

// 字符截取指定长度(str原字符串，rep替换字符串,limit长度限制)
func StringsTruncate(str, rep string, limit int) (dst string) {
	if len([]rune(str)) > limit {
		dst = string([]rune(str)[:limit]) + rep
	} else {
		dst = str
	}
	return
}

// EarthDistance 根据经纬度获取距离
//
//	参数：lat1纬度1 lng1经度1 lat2纬度2 lng2经度2
//	返回距离 米
func EarthDistance(lat1, lng1, lat2, lng2 float64) int {
	// log.Println(lat1, lng1, lat2, lng2)
	radius := 6378137.00 // 6378137
	rad := math.Pi / 180.0

	lat1 = lat1 * rad
	lng1 = lng1 * rad
	lat2 = lat2 * rad
	lng2 = lng2 * rad

	theta := lng2 - lng1
	dist := math.Acos(math.Sin(lat1)*math.Sin(lat2) + math.Cos(lat1)*math.Cos(lat2)*math.Cos(theta))

	return int(dist * radius)
}

// GetRand 获取短信验证码随机数
func GetRand() string {
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	code := fmt.Sprintf("%06v", rnd.Int31n(1000000))
	return code
}

// GetIntersection 获取多个字符串数组的交集
func GetIntersection(arrays ...[]string) []string {
	// 创建一个 map 用于存储每个字符串出现的次数
	counts := make(map[string]int)
	// 遍历每个数组，统计字符串出现的次数
	for _, array := range arrays {
		for _, str := range array {
			counts[str]++
		}
	}
	// 创建一个结果数组
	var intersection []string
	// 检查每个字符串是否在每个数组中都出现过，如果是，则加入结果数组
	for str, count := range counts {
		if count == len(arrays) {
			intersection = append(intersection, str)
		}
	}
	// 对结果数组进行排序
	sort.Strings(intersection)
	return intersection
}

// Arrcmp 查找两个数组的异同
func Arrcmp(src []int64, dest []int64) ([]int64, []int64) {
	msrc := make(map[int64]byte) // 按源数组建索引
	mall := make(map[int64]byte) // 源+目所有元素建索引
	var set []int64              // 交集
	// 1.源数组建立map
	for _, v := range src {
		msrc[v] = 0
		mall[v] = 0
	}
	// 2.目数组中，存不进去，即重复元素，所有存不进去的集合就是并集
	for _, v := range dest {
		l := len(mall)
		mall[v] = 1
		if l != len(mall) { // 长度变化，即可以存
			l = len(mall)
		} else { // 存不了，进并集
			set = append(set, v)
		}
	}
	// 3.遍历交集，在并集中找，找到就从并集中删，删完后就是补集（即并-交=所有变化的元素）
	for _, v := range set {
		delete(mall, v)
	}
	// 4.此时，mall是补集，所有元素去源中找，找到就是删除的，找不到的必定能在目数组中找到，即新加的
	var added, deleted []int64
	for v, _ := range mall {
		_, exist := msrc[v]
		if exist {
			deleted = append(deleted, v)
		} else {
			added = append(added, v)
		}
	}
	return added, deleted
}

// SliceDel 删除切片中的元素
func SliceDel[S ~[]E, E comparable](s S, v E) (res S) {
	// 找到v.GroupID在clearGroups中的索引
	idx := slices.Index(s, v) // slices.Index也是Go 1.21+新增的，用于查找元素索引
	if idx != -1 {            // 确保元素存在
		// 删除索引idx处的元素（删除区间为[idx, idx+1)）
		res = slices.Delete(s, idx, idx+1)
		return res
	}
	return s
}
