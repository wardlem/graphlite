package data

import (
	"github.com/wardlem/graphlite/util"
	//"fmt"
	//"os"
)

// error messages
const (
    nilClass = "attempt to operate on a nil class"
)

const (
    classDataSize = 9
)

type Class struct {
	Id        uint8  // The id of the Class
	Count     uint32 // The number of Vertices that belong to this Class
	label     uint16 // The id of the Label for the name of the Class
	super     uint8  // The Id of the parent Class for this Class
	sub       uint8  // The Id of the first child class for the class
	nextSub   uint8  // The Id of the next child Class for the Parent Class
	index     *classIdIndex // Keeps track of what vertices belong to this class
	// i map[string]Index // A map of all the indices for the Class
}

func constructClass(id uint8, bytes []byte) (*Class, *DataError) {
    if len(bytes) != 8 {
        return nil, dataError("Attempt to construct class with slice of improper size", nil, nil)
    }
    var e error
	c := new(Class)
	count := bytes[0:4]
	label := bytes[4:6]
	super := bytes[6]
	sub := bytes[7]
	nextSub := bytes[8]

	c.Id = id
	c.Count, e = util.BytesToUint32(count)
	if (e != nil){
	    return nil, dataError("Failed to construct class. Could not convert count.", e, nil)
	}
	c.label, e = util.BytesToUint16(label)
	if (e != nil){
	    return nil, dataError("Failed to construct class. Could not convert label.", e, nil)
	}
	c.super = super
	c.sub = sub
	c.nextSub = nextSub

	return c, nil
}

func createClass(id uint8, label uint16, super *Class, g *Graph) *Class {
    
    var c = new(Class)
    c.Id = id
    c.Count = uint32(0)
    c.label = label
    c.sub = uint8(0)
    if super == nil {
        c.super = uint8(0)
        c.nextSub = uint8(0)
    } else {
        c.super = super.Id
        c.nextSub = super.sub
        super.sub = c.Id
    }
    c.createIdIndex(g)
    
    return c
}

func (c *Class) Data() []byte {
    bytes := make([]byte, 0, classDataSize)
    count, _ := util.Uint32ToBytes(c.Count)
    label, _ := util.Uint16ToBytes(c.label)
    
    bytes = append(bytes, count...)
    bytes = append(bytes, label...)
    bytes = append(bytes, c.super, c.sub, c.nextSub)
    return bytes;
}

func (c *Class) Label(g *Graph) (*Label, *DataError) {
    Assert(nilClass, c != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    
    return g.labelStore.find(c.label)
}

func (c *Class) Name(g *Graph) (string, *DataError) {
    label, err := c.Label(g)
    if err != nil {
        return "", err
    }
    return label.Value(g), nil
}

func (c *Class) Super(g *Graph) *Class {
    return g.classStore.Find(c.super)
}

func (c *Class) Sub(g *Graph) *Class {
    return g.classStore.Find(c.sub)
}

func (c *Class) NextSub (g *Graph) *Class {
    return g.classStore.Find(c.nextSub)
}

func (c *Class) hasId (id uint32, g *Graph) bool {
    if (c.index == nil) {
        c.openIdIndex(g)
    }
    
    if c.index.hasId(id) {
        return true
    }
    
    subClass := c.Sub(g)
    for subClass != nil {
        if subClass.hasId(id, g) {
            return true
        }
        subClass = subClass.NextSub(g)
    }
    
    return false
}

func (c *Class) openIdIndex(g *Graph) {
    className, _ := c.Name(g)
    idxFileName := className + ".idx"
    fileName := g.indexPath(idxFileName)
    
    c.index, _ = constructClassIdIndex(fileName)
}

func (c *Class) createIdIndex(g *Graph) {
    className, _ := c.Name(g)
    idxFileName := className + ".idx"
    fileName := g.indexPath(idxFileName)
    
    c.index, _ = createClassIdIndex(fileName)
}
