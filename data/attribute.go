package data

import (
	"github.com/wardlem/graphlite/util"
)

// error messages
const (
    nilAttribute = "attempt to operate on nil attribute"
    keyRetrievalError = "error while retrieving attribute key"
    zeroAttributeId = "attribute had an id of 0 when it should not have"
)

const (
    empty_t = 0x00
    integer_t = 0x01
    real_t = 0x02
    boolean_t = 0x03
    text_t = 0x04
    list_t = 0x05
    map_t = 0x06
)

const attributeDataSize = 15

type Attribute struct {
	Id    uint32 // the id of the attribute
	label uint16 // the id of the label for this attribute
	t     byte   // the type of this label
	data  []byte // the raw data for the value of this attribute; always 8 bytes
	next  uint32 // the id of the next attribute for the owner of this attribute
}

func constructAttribute(id uint32, bytes []byte) (*Attribute, *DataError) {
	util.Assert(
	    "An attribute can only be constructed with a slice of the proper length",
		len(bytes) == 15)
	
	var e error
	att := new(Attribute)
	label := bytes[0:2]
	t := bytes[2]
	data := bytes[3:11]
	next := bytes[11:15]
	att.Id = id
	att.label, e = util.BytesToUint16(label)
	if e != nil {
		return nil, dataError("Failed to construct attribute. Could not convert label.", e, nil)
	}
	att.t = t
	att.data = data
	att.next, e = util.BytesToUint32(next)
	if e != nil {
		return nil, dataError("Failed to construct attribute. Could not convert next.", e, nil)
	}
	return att, nil
}

func (attr *Attribute) Value(g *Graph) (val Any, err *DataError) {
	var e error
	var id uint64
	switch attr.t {
	case empty_t:
		return nil, dataError("Failure to convert attribute value. Unsupported type found.", nil, nil)
		return
	case integer_t:
		val, e = util.BytesToUint64(attr.data)
		if e != nil {
			return nil, dataError("Failed to convert attribute value. Could not convert integer.", e, nil)
		}
	case real_t:
		val = util.BytesToFloat64(attr.data)
	case boolean_t:
		val, e = util.BytesToUint64(attr.data)
		if e != nil {
			return nil, dataError("Failed to convert attribute value. Could not convert boolean.", e, nil)
		}
		if val == 0 {
			val = false
		} else {
			val = true
		}
	case text_t:
		val = ""
		// todo -> get from string store
	case list_t:
		id, e = util.BytesToUint64(attr.data)
		if e != nil {
			return nil, dataError("Failed to convert attribute value. Could not convert list.", e, nil)
		}
		val = constructList(uint32(id))

	case map_t:
		id, e = util.BytesToUint64(attr.data)
		if e != nil {
			return nil, dataError("Failed to convert attribute value. Could not convert map.", e, nil)
		}
		val = constructMap(uint32(id))
	}
	return
}

func (a *Attribute) IsInteger() bool {
	return a.t == integer_t
}

func (a *Attribute) IsText() bool {
	return a.t == text_t
}

func (a *Attribute) IsBoolean() bool {
	return a.t == boolean_t
}

func (a *Attribute) IsReal() bool {
	return a.t == real_t
}

func (a *Attribute) IsList() bool {
	return a.t == list_t
}

func (a *Attribute) Label(g *Graph) (*Label, *DataError) {
	Assert (nilAttribute, a != nil)
	Assert (nilGraph, g != nil)
	Assert (nilLabelStore, g.labelStore != nil)
	
	return g.labelStore.find(a.label)
}

func (a *Attribute) Key(g *Graph) (string, *DataError) {
    Assert(nilAttribute, a != nil)
    
    l, e := a.Label(g)
    if e != nil {
        return "", dataError(keyRetrievalError, nil, e)
    }
    return l.Value(g), nil
}

func (a *Attribute) Next(g *Graph) *Attribute {
	Assert(nilAttribute, a != nil)
	Assert(nilGraph, g != nil)
	Assert(nilAttributeStore, g.attributeStore != nil)
	
	if a.next == uint32(0) {
	    return nil
	}
	
	return g.attributeStore.Find(a.next)
	
}


