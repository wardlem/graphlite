package data

import (
    "os"
)

// error messages
const (
    nilAttributeStore = "attempt to operate on nil attribute store"
    nilAttributeTrackingMap = "attempt to operate on a nil attribute store tracking map"
    nilAttributeIdStore = "attempt to operate on a nil attribute id store"
)

// The attribute store manages the persistence of vertex and edge attributes.
type attributeStore struct {
    file *os.File
    idStore *uint32IdStore
    tracking map[uint32]*Attribute
}

// Creates and prepares an existing attribute store
func constructAttributeStore (g *Graph) (*attributeStore, *DataError) {
    Assert(nilGraph, g != nil)
    s := new(attributeStore)
    fileName := g.storePath("attribute")
    
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); (e != nil) {
        return nil, dataError("Could not open file for attribute store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    fileName = g.storePath("attribute.id")
    
    idStore, de := constructUint32IdStore(fileName)
    if (de != nil){
        return nil, de
    }
    s.idStore = idStore
    
    s.tracking = make(map[uint32]*Attribute, 0)
    
    return s, nil;
}

// Creates and prepares an attribute store that does not yet exist.
func createAttributeStore (g *Graph) (*attributeStore, *DataError) {
    Assert(nilGraph, g != nil)
    s := new(attributeStore)
    fileName := g.storePath("attribute")
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); (e != nil){
        return nil, dataError("Could not create file for attribute store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    
    fileName = g.storePath("attribute.id")
    if idStore, de := createUint32IdStore(fileName); (de != nil){
        return nil, de
    } else {
        s.idStore = idStore
    }
    
    s.tracking = make(map[uint32]*Attribute, 0)
    
    return s, nil;
}

// Finds an attribute by id and returns it.
func (s *attributeStore) Find(id uint32) *Attribute {
    Assert(nilAttributeStore, s != nil)
    Assert(zeroAttributeId, id != uint32(0))
    Assert(nilAttributeTrackingMap, s.tracking != nil)

    if a, ok := s.tracking[id]; ok {
        return a
    }
    
    // read the vertex from the file and return it
    readAt := int64((id - 1) * attributeDataSize)
    bytes := make([]byte, attributeDataSize)
    _, e := s.file.ReadAt(bytes, readAt)
    if (e != nil) {
        return nil
    }
    
    a, _ := constructAttribute(id, bytes) 
    return a
}

// Let's the store know that the attribute has changes that need to be written.
func (s *attributeStore) Track(a *Attribute) {
    Assert(nilAttributeStore, s != nil)
    Assert(nilAttribute, a != nil)
    Assert (zeroAttributeId, a.Id != uint32(0))
    Assert(nilAttributeTrackingMap, s.tracking != nil)
    
    s.tracking[a.Id] = a
    
}

// Removes an attribute from the store.
func (s *attributeStore) Remove(a *Attribute) {
    Assert(nilAttributeStore, s != nil)
    Assert(nilAttribute, a != nil)
    Assert (zeroAttributeId, a.Id != uint32(0))
    Assert(nilAttributeTrackingMap, s.tracking != nil)
    Assert(nilAttributeIdStore, s.idStore != nil)
    
    id := a.Id
    s.idStore.addId(id)
    a.t = empty_t
    
    s.tracking[a.Id] = a
    
}

func (s *attributeStore) nextId() uint32 {
    Assert(nilAttributeStore, s != nil)
    Assert(nilAttributeIdStore, s.idStore != nil)
    return s.idStore.nextId()
}

func (store *attributeStore) shutdown () {
    if (store.idStore != nil){
        store.idStore.shutdown()
    }
    if (store.file != nil){
        _ = store.file.Close()
    }
}





