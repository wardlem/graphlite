package main

// DB represents the database itself.
type DB struct {
	format   string                 // The output format
	using    string                 // Which graph is being used
	graphs   map[string]*Graph       // All the graphs stored in the database
    settings map[string]Any         // A map of stored database settings
	vars     map[string]Entity // Vars that are saved during execution
	//commands []map[string]Command   // Commands for the Database
}

// init initializes the database object.
func (db *DB) init (format string, use string) error {
    db.format = format
    if (use != "") {
        db.using = use
        g, e := db.G(use)
        if e != nil {
            return e
        }
        db.vars["G"] = g
    }
    
    db.vars["DB"] = db
    
    return nil
}

// G loads a graph object from memory or from disc.
func (db *DB) G (name string) (*Graph, error) {
    if g, ok := db.graphs[name]; ok {
        return g, nil
    }
    
    g, e := db.loadGraph(name)
    db.graphs[name] = g
    return g, e
}

// loadGraph loads a graph from disc
func (db *DB) loadGraph(name string) (*Graph, error){
    // TODO Actually load a graphs
    return nil, nil
}

// Performs a user command
func (db *DB)command([]string) Entity {
    // TODO Do something real with this
    return db
}
