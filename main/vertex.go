package main

// Vertex is a vertex (node) entity in a graph.
type Vertex struct {
	id    int32
	class *Class
	atts  []Any
}
