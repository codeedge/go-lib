//go:build wireinject
// +build wireinject

package main

import (
	"github.com/google/wire"
)

var SuperSet = wire.NewSet(NewStudent, NewClass, NewSchool)

func initSchool() (School, error) {
	wire.Build(SuperSet)
	return School{}, nil
}
