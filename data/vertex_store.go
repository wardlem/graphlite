package data

import (
    "os" // file operations
    
    "github.com/wardlem/graphlite/util" // for file permissions
)

// error messages 
const (
    nilVertexStore = "attempt to operate on nil vertex store"
    zeroVertexId = "can not track a vertex with id of 0"
    nilVertexTrackingMap = "attempt to operate on nil vertex tracking map"
)

// The vertex store is responsible for managing the persistence of all
// vertices for the graph.
type vertexStore struct {
    file *os.File
    idStore *uint32IdStore
    tracking map[uint32]*Vertex
}

func constructVertexStore(g *Graph) (*vertexStore, *DataError){
    Assert(nilGraph, g != nil)
    
    s := new(vertexStore)
    fileName := g.storePath("vertex")
    
    if file, e := os.OpenFile(fileName, os.O_RDWR, util.FilePermission); (e != nil) {
        return nil, dataError("Could not open the file for a vertex store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    fileName = g.storePath("attribute.id")
    
    idStore, de := constructUint32IdStore(fileName)
    if (de != nil){
        return nil, de
    }
    s.idStore = idStore
    
    s.tracking = make(map[uint32]*Vertex, 0)
    
    return s, nil;
}

func createVertexStore(g *Graph) (*vertexStore, *DataError){
    Assert(nilGraph, g != nil)
    
    s := new(vertexStore)
    fileName := g.storePath("vertex")
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, util.FilePermission); (e != nil){
        return nil, dataError("Could not create file for a vertex store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    
    fileName = g.storePath("vertex.id")
    if idStore, de := createUint32IdStore(fileName); (de != nil){
        return nil, de
    } else {
        s.idStore = idStore
    }
    
    s.tracking = make(map[uint32]*Vertex, 0)
    
    return s, nil;
}

func (s *vertexStore) Track (v *Vertex) {
    Assert(nilVertexStore, s != nil)
    Assert(nilVertex, v != nil)
    Assert (zeroVertexId, v.Id != uint32(0))
    Assert(nilVertexTrackingMap, s.tracking != nil)
    
    s.tracking[v.Id] = v
}

func (s *vertexStore) Find (id uint32) *Vertex {
    Assert(nilVertexStore, s != nil)
    Assert(zeroVertexId, id != uint32(0))
    Assert(nilVertexTrackingMap, s.tracking != nil)

    if v, ok := s.tracking[id]; ok {
        return v
    }
    
    // read the vertex from the file and return it
    readAt := int64((id - 1) * vertexDataSize)
    bytes := make([]byte, vertexDataSize)
    _, e := s.file.ReadAt(bytes, readAt)
    if (e != nil) {
        return nil
    }
    
    return constructVertex(id, bytes)
    
}

func (s *vertexStore) Remove(v *Vertex, g *Graph) {
    
    s.idStore.addId(v.Id)
    g.labelStore.removeLabel(v.Key())
    
}

func (s *vertexStore) write() {
    
}

func (store *vertexStore) shutdown () {
    if (store.idStore != nil){
        store.idStore.shutdown()
    }
    if (store.file != nil){
        _ = store.file.Close()
    }
}
