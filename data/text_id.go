package data

import (
    "github.com/wardlem/graphlite/util"
)

// Error messages
const(
    wrongTextIdDataSize = "can not construct a text id of the wrong size"
    constructTextIdFail = "failed to construct text id"
    nilTextId = "attempt to operate on nil text id"
    
)

const (
    textIdDataSize = 12 // the size in bytes of a text id in storage
)

// A text id holds both the id and row count (a row being a predefined quantity
// of bytes) of a text object.
type textId struct {
    value uint64
    rows uint32 // the number of rows the tet takes
}

// Creates an existing text id from byte data
func constructTextId(bytes []byte) (*textId, *DataError) {
    Assert(wrongTextIdDataSize, len(bytes) == textIdDataSize)
    
    var e error
    id := new(textId)
    
    value := bytes[0:8]
    rows := bytes[8:12]
    
    if id.value, e = util.BytesToUint64(value); e != nil {
        dataError(constructTextIdFail, e, nil)
    }
    
    if id.rows, e = util.BytesToUint32(rows); e != nil {
        dataError(constructTextIdFail, e, nil)
    }
    
    return id, nil
    
}

// Responsible for creating a new text id from scratch.
func newTextId(value uint64, rows uint32) *textId {
    id := new(textId)
    id.value = value
    id.rows = rows
    
    return id
}

// Returns the text id's byte representation for storage.
func (id *textId) data () []byte {
    Assert(nilTextId, id != nil)
    bytes, _ := util.Uint64ToBytes(id.value)
    
    rows, _ := util.Uint32ToBytes(id.rows)
    bytes = append(bytes, rows...)
    
    return bytes
    
}



