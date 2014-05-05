package main

// Graph represents a graph stored in the database.
type Graph struct {
	db       *DB // The database the graph belongs to
	names    string
	classes  []*Class         // All the classes in the graph
	classMap map[string]int   // Index of the class names
	vCount   int32            // The number of vertices in the graph
	eCount   int32            // The number of edges in the graph
	v        map[int]Vertex   // The remembered vertices in the graph
	e        map[int]Edge     // The remembered edges in the graph
	labels   map[int]string   // The labels that belong to this graph
	stores   map[string]Store //
}

type Store interface {
	store(Entity) (Any, error) // Any is the id
	retrieve(Any)
}

// Performs a user command
func (g *Graph) command([]string) Entity {
	// TODO Do something real with this
	return g
}

