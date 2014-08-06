package data

import (
	"github.com/wardlem/graphlite/util"
)

// error messages
const (
    nilVertex = "attempt to operate on a nil vertex"
)

const vertexDataSize = 13


type Vertex struct {
	Id uint32 // The Id of the vertex
	class uint8 // Id of the class the vertex belongs to
	out uint32 // Id of the first outbound edge of the vertex
	in uint32 // Id of the first inbound edge of the vertex
	outMap edgeMap // map stores outbound edges by label
	inMap edgeMap // map stores inbound edges by label
	attributable
}

func constructVertex (id uint32, bytes []byte) *Vertex {
	vertex := new(Vertex)
	class := bytes[0]
	out := bytes[1:5]
	in := bytes[5:9]
	attributes := bytes[9:13]

	vertex.Id = id;
	vertex.class = class;
	vertex.out, _ = util.BytesToUint32(out)
	vertex.in, _ = util.BytesToUint32(in)
	vertex.firstAtt, _ = util.BytesToUint32(attributes)

	return vertex;
}

func newVertex(class *Class) *Vertex {
    vertex := new(Vertex)
    vertex.class = class.Id
    
    return vertex
}

func (v *Vertex) Class(g *Graph) *Class {
    Assert(nilVertex, v != nil)
    Assert(nilGraph, g != nil)
	Assert(nilClassStore, g.classStore != nil)
	
	return g.classStore.Find(v.class)
}

// Returns the name of the class the vertex belongs to
func (v *Vertex) ClassName(g *Graph) string {
    name, _ := v.Class(g).Name(g)
    return name
}

func (v *Vertex) FirstOut(g *Graph) *Edge {
	Assert(nilVertex, v != nil)
    Assert(nilGraph, g != nil)
	Assert(nilEdgeStore, g.edgeStore != nil)
	
	return g.edgeStore.Find(v.out)
}

func (v *Vertex) Out(g *Graph) edgeMap {
    if v.outMap == nil {
        e := v.FirstOut(g)
        m := make(edgeMap)
        for e != nil {
            m.add(e, g)
            e = e.OutNext(g)
        }
        v.outMap = m
    }
    
    return v.outMap

}

func (v *Vertex) FirstIn(g *Graph) *Edge {
	Assert(nilVertex, v != nil)
    Assert(nilGraph, g != nil)
	Assert(nilEdgeStore, g.edgeStore != nil)
	
	return g.edgeStore.Find(v.in)
}

func (v *Vertex) In(g *Graph) edgeMap {
    if v.inMap == nil {
        e := v.FirstIn(g)
        m := make(edgeMap)
        for e != nil {
            m.add(e, g)
            e = e.InNext(g)
        }
        v.inMap = m
    }
    
    return v.inMap
}

func (v *Vertex) RemoveOutboundEdge(e *Edge, g *Graph) {
    m := v.Out(g)
        if v.out == e.Id {
            v.out = e.outNext
            g.vertexStore.Track(v)
        } else {
            OutMapLoop:
            for _, list := range m {
                for _, edge := range list {
                    if edge.outNext == e.Id {
                        edge.outNext = e.outNext
                        g.edgeStore.Track(edge)
                        break OutMapLoop
                    }
                }
            }
        }
        m.remove(e, g)
}

func (v *Vertex) RemoveInboundEdge(e *Edge, g *Graph) {
    m := v.In(g)
        if v.in == e.Id {
            v.in = e.inNext
            g.vertexStore.Track(v)
        } else {
            InMapLoop:
            for _, list := range m {
                for _, edge := range list {
                    if edge.inNext == e.Id {
                        edge.inNext = e.inNext
                        break InMapLoop
                    }
                }
            }
        }
        m.remove(e, g)
}

func (v *Vertex) RemoveEdge(e *Edge, g *Graph) {
    if (e.from == v.Id) {
        v.RemoveOutboundEdge(e, g)
    }
    if (e.to == v.Id) {
        v.RemoveInboundEdge(e, g)
    }
}

func (v *Vertex) track(g *Graph) {
    g.vertexStore.Track(v)
}


