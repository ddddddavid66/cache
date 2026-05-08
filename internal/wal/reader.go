package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"
)

var ErrCRC = errors.New("crc mismatch: ")

type Reader struct {
	f  *os.File
	br *bufio.Reader
}

func NewReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open wal for read: %w", err)
	}
	return &Reader{
		f:  f,
		br: bufio.NewReaderSize(f, 64*1024), //64KB缓存
	}, nil
}

func (r *Reader) readOne() (Record, error) {
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r.br, header); err != nil {
		return Record{}, err
	}
	typ := (RecordType)(header[4])
	keyLen := binary.LittleEndian.Uint16(header[5:7])
	valueLen := binary.LittleEndian.Uint32(header[7:11])
	ttl := time.Duration(binary.LittleEndian.Uint64(header[11:19]))
	version := int64(binary.LittleEndian.Uint64(header[19:27]))
	expiredAt := int64(binary.LittleEndian.Uint64(header[27:35]))

	//读取key + Value
	body := make([]byte, int(keyLen)+int(valueLen))
	if _, err := io.ReadFull(r.br, body); err != nil {
		return Record{}, nil
	}

	//CRC校验
	stored := binary.LittleEndian.Uint32(header[:4])
	actual := crc32.ChecksumIEEE(header[4:])
	actual = crc32.Update(actual, crc32.IEEETable, body)
	if stored != actual {
		return Record{}, fmt.Errorf("%w : stored : %08x actual: %08x", ErrCRC, stored, actual)
	}
	//计算key
	key := string(body[:keyLen])
	var value []byte
	if valueLen > 0 {
		value = body[keyLen:]
	}

	return Record{
		Type:      typ,
		Key:       key,
		Value:     value,
		TTL:       ttl,
		Version:   version,
		ExpiredAt: expiredAt,
	}, nil
}

// 直到遇到CRC错误
func (r *Reader) ReadAll() ([]Record, error) {
	var list []Record
	for {
		rec, err := r.readOne()
		if err == io.EOF {
			break
		}
		if err != nil {
			break //说明CRC 错误 磁盘文件坏了  写入中途崩溃
		}
		list = append(list, rec)
	}
	return list, nil
}

func (r *Reader) Close() error {
	return r.f.Close()
}
