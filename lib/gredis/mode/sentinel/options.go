package sentinel

import (
	"github.com/codeedge/go-lib/lib/gredis/mode"
	
)

type options struct {
	addrs            []string
	masterName       string
	poolOpts         []mode.PoolOption
	dialOpts         []redis.DialOption
	sentinelDialOpts []redis.DialOption
}

type OptFunc func(opts *options)

func Addrs(value []string) OptFunc {
	return func(opts *options) {
		opts.addrs = value
	}
}

func MasterName(value string) OptFunc {
	return func(opts *options) {
		opts.masterName = value
	}
}

func PoolOpts(value ...mode.PoolOption) OptFunc {
	return func(opts *options) {
		for _, poolOpt := range value {
			opts.poolOpts = append(opts.poolOpts, poolOpt)
		}
	}
}

func DialOpts(value ...redis.DialOption) OptFunc {
	return func(opts *options) {
		for _, dialOpt := range value {
			opts.dialOpts = append(opts.dialOpts, dialOpt)
		}
	}
}

func SentinelDialOpts(value ...redis.DialOption) OptFunc {
	return func(opts *options) {
		for _, dialOpt := range value {
			opts.sentinelDialOpts = append(opts.sentinelDialOpts, dialOpt)
		}
	}
}
