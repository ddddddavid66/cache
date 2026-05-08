package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sync"
	"time"
)

type RecordType uint8

const (
	RecordSet               RecordType = 1
	RecordDelete            RecordType = 2
	RecordShadowRetrySet    RecordType = 3
	RecordShadowRetryDelete RecordType = 4
)

type Record struct {
	Type      RecordType
	Key       string
	Value     []byte
	TTL       time.Duration
	Version   int64
	ExpiredAt int64 //UnixNano 0 不过期
}

// WAL 文件格式 CRC32 + TYPE + KeyLen + ValueLen + TTL + Version + Expired+  Key + Value
//   4 + 1 +2 + 4 +8 +8 + 8

// Write
type Writer struct {
	mu       sync.Mutex
	f        *os.File
	bw       *bufio.Writer
	path     string
	fileSize int64
	maxBytes int64 //单文件最大字节 超过以后需要手动切割
}

const (
	defaultMaxBytes = 256 * 1024 * 1024 //256MB
	headerSize      = 35                // 4 +1 + 2 + 4 + 8 +8 +8
)

func NewWriter(path string, maxBytes int64) (*Writer, error) {
	if maxBytes < 0 {
		maxBytes = defaultMaxBytes
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("open wal: %w", err)
	}
	//获取文件大小
	info, _ := f.Stat()
	return &Writer{
		f:        f,
		bw:       bufio.NewWriterSize(f, 64*1024), //64KB 缓冲
		path:     path,
		fileSize: info.Size(),
		maxBytes: maxBytes,
	}, nil
}

// 追加记录
func (w *Writer) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	//检查是否需要切割
	if w.fileSize >= w.maxBytes {
		if err := w.rotate(); err != nil {
			return nil
		}
	}

	// 编码
	keyBytes := []byte(rec.Key)
	valueLen := len(rec.Value)

	//构造header  先填冲0
	buf := make([]byte, headerSize+valueLen)

	buf[4] = byte(rec.Type) // type
	// 注意是小端序填
	binary.LittleEndian.PutUint16(buf[5:7], uint16(len(rec.Key)))
	binary.LittleEndian.PutUint32(buf[7:11], uint32(valueLen))
	binary.LittleEndian.PutUint64(buf[11:19], uint64(rec.TTL))
	binary.LittleEndian.PutUint64(buf[19:27], uint64(rec.Version))
	binary.LittleEndian.PutUint64(buf[27:35], uint64(rec.ExpiredAt))

	//header 之后追加
	buf = append(buf[:headerSize], keyBytes...)
	buf = append(buf, rec.Value...)

	//计算CRC32
	checkSum := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[0:4], checkSum)

	//写入WAL
	n, err := w.bw.Write(buf)
	if err != nil {
		return fmt.Errorf("wal write: %w", err)
	}
	w.fileSize += int64(n)
	return nil
}

// Sync 刷盘
func (w *Writer) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Sync()
}

func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.bw.Flush(); err != nil {
		return err
	}
	return w.f.Close()
}

// 切割文件 rename 当前文件，创建新文件
func (w *Writer) rotate() error {
	if err := w.bw.Flush(); err != nil {
		return err
	}
	//没有Sync 旧文件数据没落盘可能就 rename 了，新文件又是空的。
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	// rename 时间戳 second 加 序号
	newPath := rotatedPath(w.path, time.Now())
	if err := os.Rename(w.path, newPath); err != nil {
		return err
	}
	f, err := os.OpenFile(w.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	w.f = f
	w.fileSize = 0
	w.bw.Reset(f) // 复用
	return nil
}

func rotatedPath(path string, now time.Time) string {
	base := path + "." + now.Format("20060102150405")
	for i := 0; ; i++ {
		if i == 0 {
			if _, err := os.Stat(base); os.IsNotExist(err) {
				return base
			}
			continue
		}
		candidate := fmt.Sprintf("%s.%06d", base, i)
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}
