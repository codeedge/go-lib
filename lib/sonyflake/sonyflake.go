package sonyflake

import (
	"github.com/sony/sonyflake"
)

var SonyId *Sonyflake

type Sonyflake struct {
	sf *sonyflake.Sonyflake
}

// Init 使用单独节点id初始化
func Init(workerID int) {
	sf := sonyflake.NewSonyflake(sonyflake.Settings{
		MachineID: func() (uint16, error) {
			return uint16(workerID), nil
		},
	})
	if sf == nil {
		panic("sonyflake not created")
	}
	SonyId = &Sonyflake{sf}
}

// GenerateMachineID 根据数据中心 ID 和机器 ID 生成唯一的机器 ID
// func GenerateMachineID(datacenterID, machineID int) int {
//	if datacenterID > 31 || machineID > 31 {
//		return 0
//	}
//	// 数据中心 ID 左移 5 位，然后与机器 ID 进行按位或运算
//	return (datacenterID << 5) | machineID
// }

// func Init2(dataCenterId, machineId int) {
//	mid := GenerateMachineID(dataCenterId, machineId)
//	sf := sonyflake.NewSonyflake(sonyflake.Settings{
//		MachineID: func() (uint16, error) {
//			return uint16(mid), nil
//		},
//	})
//	if sf == nil {
//		panic("sonyflake not created")
//	}
//	SonyId = &Sonyflake{sf}
// }

func (s *Sonyflake) GenString() (string, error) {
	id, err := s.sf.NextID()
	if err != nil {
		return "", err
	}
	return IntToBase62(id), nil
}

func (s *Sonyflake) GenUint64() (uint64, error) {
	return s.sf.NextID()
}

func (s *Sonyflake) Gen() (int64, error) {
	id, err := s.sf.NextID()
	return int64(id), err
}
