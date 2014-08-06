package data

import (
    "github.com/wardlem/graphlite/util"
)

const (
    nilText = "attempt to operate on nil text struct"
)

// Text represents any database string value used for any purpose.
type Text struct {
    Id uint64       // the id of the text struct
    length uint32   // the size of the text value in bytes when retrieved from the database
    value string    // the value of the text struct
}

func constructText(id uint64, length uint32, bytes []byte) *Text {
    t := new(Text)
    
    t.Id = id
    t.length = length
    t.value = string(bytes)
    
    return t
}

func newText(value string) *Text {
    t := new(Text)
    t.value = value
    return t
}

func (t *Text) Value() string {
    Assert(nilText, t != nil)
    return t.value
}

func (t *Text) Len() uint32 {
    Assert(nilText, t != nil)
    return uint32(len(t.value))
}

func (t *Text) data() []byte {
    Assert(nilText, t != nil)
    bytes, _ := util.Uint32ToBytes(t.Len()) // TODO do not ignore error
    valBytes := []byte(t.value)
    bytes = append(bytes, valBytes...)
    
    return bytes
}

