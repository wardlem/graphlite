package data

// error messages
const (
    nilEdgeList = "attempt to operate on a nil edge list"
)

type edgeList map[uint32]*Edge

func (m edgeList) add (e *Edge) {
    Assert (nilEdgeList, m != nil)
    Assert (nilEdge, e != nil)
     
    m[e.Id] = e
}

func (m edgeList) remove (e *Edge) {
    Assert (nilEdgeList, m != nil)
    Assert (nilEdge, e != nil)
    
    delete(m, e.Id)
}
