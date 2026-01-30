package exit

import (
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// PrintResourceUsage 打印当前CPU和内存占用情况
func PrintResourceUsage() {
	// 获取系统CPU使用率
	cpuPercent, err := cpu.Percent(time.Second, false)
	if err != nil {
		fmt.Printf("获取CPU信息失败: %v\n", err)
		return
	}

	// 获取内存信息
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		fmt.Printf("获取内存信息失败: %v\n", err)
		return
	}

	// 打印结果
	fmt.Printf("CPU 使用率: %.2f%%\n", cpuPercent[0])
	fmt.Printf("内存使用率: %.2f%%\n", vmStat.UsedPercent)
	fmt.Printf("总内存: %.2f GB\n", float64(vmStat.Total)/(1024*1024*1024))
	fmt.Printf("已用内存: %.2f GB\n", float64(vmStat.Used)/(1024*1024*1024))
	fmt.Printf("可用内存: %.2f GB\n", float64(vmStat.Available)/(1024*1024*1024))
}
