package stats

import (
	"log"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

type Stats struct {
	CpuStats  *cpu.TimesStat
	LoadStats *load.AvgStat
	DiskStats *disk.UsageStat
	MemStats  *mem.VirtualMemoryStat

	TaskCount int
}

func (s *Stats) MemUsedKb() uint64 {
	return s.MemStats.Total - s.MemStats.Available
}

func (s *Stats) MemUsedPercent() uint64 {
	return s.MemStats.Available / s.MemStats.Total
}

func (s *Stats) MemAvailableKb() uint64 {
	return s.MemStats.Available
}

func (s *Stats) MemTotalKb() uint64 {
	return s.MemStats.Total
}

func (s *Stats) DiskTotal() uint64 {
	return s.DiskStats.Total
}

func (s *Stats) DiskFree() uint64 {
	return s.DiskStats.Free
}

func (s *Stats) DiskUsed() uint64 {
	return s.DiskStats.Used
}

func (s *Stats) CpuUsage() float64 {
	idle := s.CpuStats.Idle + s.CpuStats.Iowait
	nonIdle := s.CpuStats.User + s.CpuStats.Nice + s.CpuStats.System + s.CpuStats.Irq + s.CpuStats.Softirq + s.CpuStats.Steal
	total := idle + nonIdle
	if total == 0 {
		return 0.00
	}

	return (float64(total) - float64(idle)) / float64(total)
}

func GetStats() *Stats {
	return &Stats{
		MemStats:  GetMemoryStats(),
		DiskStats: GetDiskStats(),
		CpuStats:  GetCpuStats(),
		LoadStats: GetLoadAvgStats(),
	}
}

func GetMemoryStats() *mem.VirtualMemoryStat {
	memoryStats, err := mem.VirtualMemory()
	if err != nil {
		log.Printf("[stats] Error getting memory stats %v\n", err)
		return &mem.VirtualMemoryStat{}
	}

	return memoryStats
}

func GetDiskStats() *disk.UsageStat {
	diskStats, err := disk.Usage("/")
	if err != nil {
		log.Printf("[stats] Error getting disk stats %v\n", err)
		return &disk.UsageStat{}
	}

	return diskStats
}

func GetCpuStats() *cpu.TimesStat {
	stats, err := cpu.Times(false)
	if err != nil {
		log.Printf("[stats] Error getting cpu stats %v\n", err)
		return &cpu.TimesStat{}
	}
	if len(stats) == 1 {
		return &stats[0]
	}

	return &cpu.TimesStat{}
}

func GetLoadAvgStats() *load.AvgStat {
	loadStats, err := load.Avg()
	if err != nil {
		log.Printf("[stats] Error getting load avg %v\n", err)
		return &load.AvgStat{}
	}

	return loadStats
}
