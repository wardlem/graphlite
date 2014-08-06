package data

// error messages
const (
    nilEdgeMap = "attempt to operate on a nile edge map"
)

type edgeMap map[string]edgeList

func (m edgeMap) add (e *Edge, g *Graph) {
    Assert (nilEdgeMap, m != nil)
    Assert (nilEdge, e != nil)
    Assert (nilGraph, g != nil)
    Assert (nilLabelStore, g.labelStore != nil)
    
    key := e.Key(g)
    
    if l, ok := m[key]; ok {
        l.add(e)
    } else {
        m[key] = make(edgeList)
        l = m[key]
        l.add(e)
    }
}

func (m edgeMap) remove (e *Edge, g *Graph) {
    Assert (nilEdgeMap, m != nil)
    Assert (nilEdge, e != nil)
    Assert (nilGraph, g != nil)
    Assert (nilLabelStore, g.labelStore != nil)
    
    key := e.Key(g)
    
    if l, ok := m[key]; ok {
        l.remove(e)
        if len(l) == 0 {
            delete(m, key)
        }
    }
}

func (m edgeMap) has (key string) bool {
    _, ok := m[key]
    return ok
}

func (m edgeMap) get (key string) edgeList {
    l, _ := m[key]
    return l
}
