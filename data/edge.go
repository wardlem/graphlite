package data

import (
	"github.com/wardlem/graphlite/util"
)

const (
    nilEdge = "attempt to operate on a nil edge"
)

const edgeDataSize = 22

type Edge struct {
	Id         uint32 // The Id of the Edge
	label      uint16 // The Id of the Label for the Edge
	from       uint32 // The Id of the origin Vertex of the Edge
	to         uint32 // The Id of the destination Vertex of the Edge
	outNext    uint32 // The Id of the next Edge of the origin Vertex
	inNext     uint32 // The Id of the next Edge of the destination Vertex
	attributable
}

func constructEdge(id uint32, bytes []byte) (*Edge, *DataError){
    var e error
	edge := new(Edge)
	label := bytes[0:2]
	from := bytes[2:6]
	to := bytes[6:10]
	outNext := bytes[10:14]
	inNext := bytes[14:18]
	attributes := bytes[18:22]
	
	edge.Id = id
	edge.label, e = util.BytesToUint16(label)
	if (e != nil){
	    return nil, dataError("Failed to construct edge. Could not convert label.", e, nil)
	}
	edge.from, e = util.BytesToUint32(from)
	if (e != nil){
	    return nil, dataError("Failed to construct edge. Could not convert from.", e, nil)
	}
	edge.to, e = util.BytesToUint32(to)
	if (e != nil){
	    return nil, dataError("Failed to construct edge. Could not convert to.", e, nil)
	}
	edge.outNext, e = util.BytesToUint32(outNext)
	if (e != nil){
	    return nil, dataError("Failed to construct edge. Could not convert outNext.", e, nil)
	}
	edge.inNext, e = util.BytesToUint32(inNext)
	if (e != nil){
	    return nil, dataError("Failed to construct edge. Could not convert inNext.", e, nil)
	}
	edge.firstAtt, e = util.BytesToUint32(attributes)
	if (e != nil){
	    return nil, dataError("Failed to construct edge. Could not convert attributes.", e, nil)
	}
    return edge, nil
}

func (e *Edge) Label(g *Graph) *Label {
    Assert(nilEdge, e != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    
    l, _ := g.labelStore.find(e.label)
    return l
}

func (e *Edge) Key(g *Graph) string {
    Assert(nilAttribute, e != nil)
    l := e.Label(g)
    return l.Value(g)
}

func (e *Edge) From (g *Graph) *Vertex {
    Assert(nilGraph, g != nil)
    Assert(nilEdge, e != nil)
    Assert(nilVertexStore, g.vertexStore != nil)
    
    return g.vertexStore.Find(e.from)
}

func (e *Edge) To(g *Graph) *Vertex {
    Assert(nilGraph, g != nil)
    Assert(nilEdge, e != nil)
    Assert(nilVertexStore, g.vertexStore != nil)
    
    return g.vertexStore.Find(e.to)
}

func (e *Edge) OutNext(g *Graph) *Edge {
    Assert(nilGraph, g != nil)
    Assert(nilEdge, e != nil)
    Assert(nilEdgeStore, g.edgeStore != nil)
    
    return g.edgeStore.Find(e.outNext)
}

func (e *Edge) InNext(g *Graph) *Edge {
    Assert(nilGraph, g != nil)
    Assert(nilEdge, e != nil)
    Assert(nilEdgeStore, g.edgeStore != nil)
    
    return g.edgeStore.Find(e.inNext)
}

func (e *Edge) track(g *Graph) {
    g.edgeStore.Track(e)
}



