package data

import (
    "github.com/wardlem/graphlite/util"
    "os"
    //"fmt"
)

// error messages
const (
    nilGraph = "attempt to operate on a nil graph"
)

const (
    FileExtension = util.FileExtension
)

type Graph struct {
	db   *DB            // reference to the database the graph belongs to
	Name string         // the name of the graph
	classStore *classStore
	vertexStore *vertexStore
	edgeStore *edgeStore
	labelStore *labelStore
	attributeStore *attributeStore
	textStore *textStore
	mapStore *mapStore
	listStore *listStore
}

func constructGraph(db *DB, name string) (g *Graph, err *DataError) {
    Assert(nilDB, db != nil)
    
	g = new(Graph)
	g.db = db
	g.Name = name
	if g.textStore, err = constructTextStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
	if g.labelStore, err = constructLabelStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.classStore, err = constructClassStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.vertexStore, err = constructVertexStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.attributeStore, err = constructAttributeStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.mapStore, err = constructMapStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.listStore, err = constructListStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
	return g, nil
}

func createGraph(db *DB, name string) (g *Graph, err *DataError) {
    Assert(nilDB, db != nil)
    
    g = new(Graph)
    g.db = db
    g.Name = name
    if e := os.MkdirAll(g.Path(), 0777); e != nil {
        return nil, dataError("Failure to create new graph: " + g.Path(), e, nil)
    }
    if g.textStore, err = createTextStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.labelStore, err = createLabelStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.classStore, err = createClassStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    g.classStore.initialize(g)
    if g.vertexStore, err = createVertexStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.attributeStore, err = createAttributeStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.mapStore, err = createMapStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
    if g.listStore, err = createListStore(g); err != nil {
        return nil, dataError("Failure to construct graph: " + name + ".", nil, err)
    }
	return g, nil
}

func (g *Graph) C (name string) *Class{
    return g.classStore.FindByName(name, g)
}

func (g *Graph) Destroy() *DataError{
    if e := os.RemoveAll(g.Path()); e != nil{
        return dataError("Error destroying graph.", e, nil)
    }
    return nil
}

func (g *Graph) storePath(storeName string) string {
    path := g.Path()
    return path + string(os.PathSeparator) + storeName + FileExtension
}

func (g *Graph) indexPath(indexName string) string {
    path := g.Path()
    return path + string(os.PathSeparator) + "idx" + string(os.PathSeparator) + indexName + FileExtension
}

func (g *Graph) Path() string {
	return g.db.Path + "/" + g.Name
}

func (g *Graph) shutdown() {
    if (g != nil){
    
        stores := []storer{
            g.classStore,
            g.vertexStore,
            g.edgeStore,
            g.labelStore,
            g.attributeStore,
            g.textStore,
            g.mapStore,
            g.listStore,
        }
    
        for _, store := range stores {
            if store != nil {
                store.shutdown()
            }
        }
    }
    //os.Exit(1)
    
         
}

