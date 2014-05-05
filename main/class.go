package main

// Class represents a class to which vertices are assigned.
type Class struct {
	graph *Graph // The graph the class belongs to
	label *Label // The name of the class
	count int32  // The number of vertices of the class
	// idx map[string]Index
	// schema *Schema
}

// ClassStore is responsible for maintaining disc storate of Class entities.
type ClassStore struct {
    
}
