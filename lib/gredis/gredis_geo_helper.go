package gredis

import (
	"errors"
	"fmt"
	"github.com/gogf/gf/os/glog"
	"reflect"
	"strconv"
)

func NewGeoLocation(name string, longitude, latitude float64) *GeoLocation {
	return &GeoLocation{
		Name: name,
		Coord: Coord{
			Longitude: longitude,
			Latitude:  latitude,
		},
	}
}

func NewGeoRadius(raw reflect.Value, opts ...Option) (*GeoRadius, error) {
	if len(opts) == 0 {
		name, err := toString(unpackValue(raw))
		return &GeoRadius{GeoLocation: GeoLocation{Name: name}}, err
	}

	if raw.Kind() != reflect.Slice {
		return nil, errors.New(fmt.Sprintf("NewGeoRadius data fail: %v", raw.Kind()))
	}

	gr := &GeoRadius{}

	structTab := make([]bool, 4)
	structTab[_result_name] = true // name
	for _, opt := range opts {     // 指定的选项
		structTab[opt] = true
	}

	i := 0
	for idx, ok := range structTab {
		if !ok {
			continue
		}
		v := raw.Index(i)

		switch idx {
		case _result_name:
			name, err := toString(unpackValue(v))
			if err != nil {
				glog.Errorf("convert to name, raw: %v, error: %v", v, err)
			}
			gr.Name = name
		case WithDist:
			dist, err := toFloat64(unpackValue(v))
			if err != nil {
				errInfo := fmt.Sprintf("convert to distance, raw: %v, error: %v", v, err)
				glog.Errorf(errInfo)
				return nil, errors.New(errInfo)
			}
			gr.Dist = dist
		case WithHash:
			hash := toInt64(unpackValue(v))
			glog.Infof("convert to distance, raw: %v", v)
			gr.Hash = hash
		case WithCoord:
			coord, err := toCoordinate(unpackValue(v))
			if err != nil {
				errInfo := fmt.Sprintf("convert to coordinate, raw: %v, error: %v", v, err)
				glog.Errorf(errInfo)
				return nil, errors.New(errInfo)
			}
			gr.Coord = coord
		default:
			errInfo := fmt.Sprintf("invalid GEO_WITH_EXT value: %v", idx)
			glog.Errorf(errInfo)
			return nil, errors.New(errInfo)
		}

		i++
	}

	return gr, nil
}

func toString(v reflect.Value) (string, error) {
	if v.Kind() != reflect.Slice {
		return "", fmt.Errorf("to string fail: %v", v.Kind())
	}

	b := v.Bytes()
	return string(b), nil
}

func toFloat64(v reflect.Value) (float64, error) {
	s, err := toString(v)
	if err != nil {
		return 0, err
	}

	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, err
	}

	return f, nil
}

func toInt64(v reflect.Value) int64 {
	i := v.Int()
	return i
}

func toCoordinate(v reflect.Value) (Coord, error) {
	if v.Kind() != reflect.Slice || v.Len() != 2 {
		return Coord{}, fmt.Errorf("invalid data format for coordainate, %v", v)
	}

	var coord Coord
	var err error
	coord.Longitude, err = toFloat64(unpackValue(v.Index(lonIdx)))
	if err != nil {
		return coord, err
	}

	coord.Latitude, err = toFloat64(unpackValue(v.Index(latIdx)))
	if err != nil {
		return coord, err
	}

	return coord, nil
}

func unpackValue(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Interface {
		if !v.IsNil() {
			v = v.Elem()
		}
	}
	return v
}

func format2GeoRadius(val interface{}, opts ...Option) ([]*GeoRadius, error) {
	v := reflect.ValueOf(val)
	if v.Kind() != reflect.Slice {
		glog.Errorf("format2GeoRadius get wrong type, want slice")
		return nil, errors.New(fmt.Sprintf("format2GeoRadius get wrong type: %v", v.Kind()))
	}

	var (
		retList = make([]*GeoRadius, v.Len())
		err     error
	)

	for i := 0; i < v.Len(); i++ {
		retList[i], err = NewGeoRadius(unpackValue(v.Index(i)), opts...)
		if err != nil {
			glog.Errorf("fail to convert raw data to GeoRadius type")
			return nil, err
		}
	}

	return retList, nil
}

func formatPosition2Geolocation(rets []*[2]float64, members ...string) []*GeoLocation {
	var (
		posList = make([]*GeoLocation, len(rets))
	)

	for i := range rets {
		if rets[i] == nil {
			glog.Errorf("%v --> no position data", members[i])
		} else {
			posList[i] = NewGeoLocation(members[i], rets[i][lonIdx], rets[i][latIdx])
		}
	}
	return posList
}
