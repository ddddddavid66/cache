package wal

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

//**Checkpoint 策略**：
//1. 基于大小切割：单个 WAL 文件超过 256MB 时，rename 为 `.wal.时间戳`，创建新文件
//2. 基于时间切割：每小时强制切割一次（即使没到 256MB）
//3. Checkpoint触发：切割后，对当前缓存做一次全量快照（Scan 导出） 然后删除快照之前的所有旧 WAL 文件

var defaultMaxFiles = 5

type CheckPointer struct {
	walDir   string
	walPath  string
	maxAge   time.Duration //超过1h就切割
	maxFiles int           // 最多保留的旧文件
}

func NewCheckPointer(walDir string, walPath string, maxAge time.Duration, maxFiles int) *CheckPointer {
	if maxFiles <= 0 {
		maxFiles = defaultMaxFiles
	}
	return &CheckPointer{
		walDir:   walDir,
		walPath:  walPath,
		maxAge:   maxAge,
		maxFiles: maxFiles,
	}
}

func (c *CheckPointer) CleanOldFiles() error {
	entries, err := os.ReadDir(c.walDir)
	if err != nil {
		return err
	}
	var oldFiles []os.DirEntry //遍历旧文件 添加
	prefix := filepath.Base(c.walPath) + "."
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasPrefix(entry.Name(), prefix) {
			oldFiles = append(oldFiles, entry)
		}
	}

	//按照时间顺序 排序 旧的在最前面
	sort.Slice(oldFiles, func(i, j int) bool {
		iInfo, _ := oldFiles[i].Info()
		jInfo, _ := oldFiles[j].Info()
		return iInfo.ModTime().Before(jInfo.ModTime())
	})

	now := time.Now()

	for index, entry := range oldFiles {
		info, _ := entry.Info()
		age := now.Sub(info.ModTime()) //保留了多久
		//保留最新的maxFiles
		remaining := len(oldFiles) - index //还剩多少个没有处理
		if remaining <= c.maxFiles {
			break
		}
		// 超过maxAge 的 删除
		if age > c.maxAge {
			path := filepath.Join(c.walDir, entry.Name())
			if err := os.Remove(path); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *CheckPointer) ListOldFiles() ([]string, error) {
	entries, error := os.ReadDir(c.walDir)
	if error != nil {
		return nil, error
	}
	var files []string
	prefix := filepath.Base(c.walPath) + "."
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), prefix) {
			files = append(files, filepath.Join(c.walDir, entry.Name()))
		}
	}
	sort.Strings(files) //按照时间排序

	return files, nil
}
