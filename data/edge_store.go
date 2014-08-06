package data

import (
    "os"
    "github.com/wardlem/graphlite/util"
)

// error messages
const (
    nilEdgeStore = "attempt to operate on a nil edge store"
    nilEdgeIdStore = "attempt to operate on a nil edge id store"
    zeroEdgeId = "edge's id can not be 0"
    nilEdgeTrackingMap = "attempt to operate on a nil edge tracking map"
)

type edgeStore struct {
    file *os.File
    idStore *uint32IdStore
    tracking map[uint32]*Edge
}

func constructEdgeStore(g *Graph) (*edgeStore, *DataError){
    Assert(nilGraph, g != nil)
    
    s := new(edgeStore)
    fileName := g.storePath("edge")
    
    if file, e := os.OpenFile(fileName, os.O_RDWR, util.FilePermission); (e != nil) {
        return nil, dataError("Could not open file for edge store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    fileName = g.storePath("edge.id")
    
    idStore, de := constructUint32IdStore(fileName)
    if (de != nil){
        return nil, de
    }
    s.idStore = idStore
    
    s.tracking = make(map[uint32]*Edge, 0)
    
    return s, nil;
}

func creatEdgeStore(g *Graph) (*edgeStore, *DataError){
    Assert(nilGraph, g != nil)
    s := new(edgeStore)
    fileName := g.storePath("edge")
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, util.FilePermission); (e != nil){
        return nil, dataError("Could not create file for edge store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    
    fileName = g.storePath("edge.id")
    if idStore, de := createUint32IdStore(fileName); (de != nil){
        return nil, de
    } else {
        s.idStore = idStore
    }
    
    s.tracking = make(map[uint32]*Edge, 0)
    
    return s, nil;
}

// Finds a edge by id and returns it.
func (s *edgeStore) Find(id uint32) *Edge {
    Assert(nilEdgeStore, s != nil)
    Assert(zeroEdgeId, id != uint32(0))
    Assert(nilEdgeTrackingMap, s.tracking != nil)

    if e, ok := s.tracking[id]; ok {
        return e
    }
    
    // read the edge from the file and return it
    readAt := int64((id - 1) * edgeDataSize)
    bytes := make([]byte, edgeDataSize)
    _, err := s.file.ReadAt(bytes, readAt)
    if (err != nil) {
        return nil
    }
    
    e, _ := constructEdge(id, bytes) 
    return e
}

// Let's the store know that the edge has changes that need to be written.
func (s *edgeStore) Track(e *Edge) {
    Assert(nilEdgeStore, s != nil)
    Assert(nilEdge, e != nil)
    Assert (zeroEdgeId, e.Id != uint32(0))
    Assert(nilEdgeTrackingMap, s.tracking != nil)
    
    s.tracking[e.Id] = e
    
}

// Removes an edge from the store.
func (s *edgeStore) Remove(e *Edge) {
    Assert(nilEdgeStore, s != nil)
    Assert(nilEdge, e != nil)
    Assert (zeroEdgeId, e.Id != uint32(0))
    Assert(nilEdgeTrackingMap, s.tracking != nil)
    Assert(nilEdgeIdStore, s.idStore != nil)
    
    id := e.Id
    s.idStore.addId(id)
    e.label = uint16(0)
    
    s.tracking[e.Id] = e
    
}

func (store *edgeStore) shutdown () {
    if (store != nil){
        if (store.idStore != nil){
            store.idStore.shutdown()
        }
        if (store.file != nil){
            _ = store.file.Close()
        }
    }
    
}
