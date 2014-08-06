package data

import(
    "os"
    "fmt"
)

// error messages
const (
    nilDB = "attempt to operate on a nil database"
)

type DB struct {
    Path string
    graphs map[string]*Graph
    settings map[string]Any
}

func ConstructDB(path string) *DB {
    db := new(DB)
    db.Path = path
    db.graphs = make(map[string]*Graph)
    // TODO read settings file
    
    return db
}

func CreateDB(path string) (*DB, *DataError) {
    db := new(DB)
    db.Path = path
    db.graphs = make(map[string]*Graph)
    if e := os.MkdirAll(path, 0777); e != nil {
        fmt.Println("Failure to create new database")
        fmt.Println(e.Error())
        return nil, dataError("Failure to create new database: " + path, e, nil)
    }
    // TODO write settings file
    return db, nil
}

func (db *DB) DestroyGraph(name string) *DataError {
    if g, err := db.G(name); err == nil && g != nil {
        g.shutdown()
        e := g.Destroy()
        delete(db.graphs, name)
        return e
    }
    g := new(Graph)
    g.db = db
    g.Name = name
    return g.Destroy()
}

func (db *DB) CreateGraph (name string) (*Graph, *DataError) {
    g, e := createGraph(db, name)
    db.graphs[name] = g
    return g, e
}

func (db *DB) Destroy() *DataError {
    db.Shutdown()
    if e := os.RemoveAll(db.Path); e != nil {
        return dataError("Error destroying database.", e, nil)
    }
    return nil
}

// G retrieves a graph by name from the database
func (db *DB) G (name string) (*Graph, *DataError){
    var e *DataError
    var g *Graph
    var ok bool
    
    if g, ok = db.graphs[name]; ok {
        return g, nil
    }
    
    g, e = constructGraph(db, name)
    db.graphs[name] = g
    return db.graphs[name], e
}

func (db *DB) Shutdown() {
    for _, graph := range db.graphs {
        graph.shutdown()
    }
}


