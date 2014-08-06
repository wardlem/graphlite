package util

import (
	"bytes"
	"encoding/binary"
	"math"
)


func BytesToUint64(b []byte) (res uint64, err error) {
	buf := bytes.NewReader(b)
	err = binary.Read(buf, binary.LittleEndian, &res)
	return
}

func Uint64ToBytes(n uint64) (res []byte, err error) {
	buf := new (bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, n)
	res = buf.Bytes()
	return
}

func BytesToUint32(b []byte) (res uint32, err error) {
	buf := bytes.NewReader(b)
	err = binary.Read(buf, binary.LittleEndian, &res)
	return
}

func Uint32ToBytes(n uint32) (res []byte, err error) {
	buf := new (bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, n)
	res = buf.Bytes()
	return
}


func BytesToUint16(b []byte) (res uint16, err error){
	buf := bytes.NewReader(b)
	err = binary.Read(buf, binary.LittleEndian, &res)
	return
}

func Uint16ToBytes(n uint16) (res []byte, err error) {
	buf := new (bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, n)
	res = buf.Bytes()
	return
}

func BytesToFloat64(b []byte) float64 {
	bits := binary.LittleEndian.Uint64(b)
	float := math.Float64frombits(bits)
	return float
}

func Float64ToBytes(f float64) []byte {
	bits := math.Float64bits(f)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes
}
