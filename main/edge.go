package main

// Edge is an edge (relationship) entity in a graph.
type Edge struct {
    graph *Graph // The graph the edge belongs to
	id       int32 // The id othe edge
	label    Any // int16 or *Label
	from     Any // int32 or *Vertex
	to       Any // int32 or *Vertex
	fromNext Any // int32 or *Edge
	toNext   Any // int32 or *Edge
	atts     Any // int32 or *Attribute
}

// EdgeStore is responsible for managing disc storage of Edge entities.
type EdgeStore struct {
    
}




