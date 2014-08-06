package data

import(
	"testing"
	"fmt"
)

func TestDatabase (t *testing.T) {
    path := "/home/mwwardle/gotest/db"
    var db *DB
    //var g *data.Graph
    var err *DataError
    
    // create the database
    if db, err = CreateDB(path); err != nil {
        fmt.Println(err.Error())
    }
    fmt.Print(db.Path)
    
    // create the graph
    if _, err = db.CreateGraph("test_graph"); err != nil {
        t.Error(err.Trace())
    }
    
    // shut down the database
    //db.Shutdown()
    
    
    // open the graph
    g, e := db.G("test_graph")
    if (e != nil) {
        t.Error(e.Trace())
    } 
    
    ls := g.labelStore
    
    ls.rootNode().printTree("", "r", g)
    
    idb := ls.addLabel("b", g)
    ls.rootNode().printTree("", "r", g)
    ida := ls.addLabel("a", g)
    ls.rootNode().printTree("", "r", g)
    idc := ls.addLabel("c", g)
    ls.rootNode().printTree("", "r", g)
    idd := ls.addLabel("d", g)
    ls.rootNode().printTree("", "r", g)
    ide := ls.addLabel("e", g)
    ls.rootNode().printTree("", "r", g)
    idf := ls.addLabel("f", g)
    ls.rootNode().printTree("", "r", g)
    ida2 := ls.addLabel("a", g)
    ls.rootNode().printTree("", "r", g)
    idx := ls.addLabel("x", g)
    ls.rootNode().printTree("", "r", g)
    idy := ls.addLabel("y", g)
    ls.rootNode().printTree("", "r", g)
    idz := ls.addLabel("z", g)
    ls.rootNode().printTree("", "r", g)
    idaa := ls.addLabel("aa", g)
    ls.rootNode().printTree("", "r", g)
    idbb := ls.addLabel("bb", g)
    ls.rootNode().printTree("", "r", g)
    Assert("ids do not match", ida == ida2)
    
    labelA, _ := ls.find(ida)
    labelB, _ := ls.find(idb)
    labelC, _ := ls.find(idc)
    labelD, _ := ls.find(idd)
    labelE, _ := ls.find(ide)
    labelF, _ := ls.find(idf)
    
    labelX, _ := ls.find(idx)
    labelY, _ := ls.find(idy)
    labelZ, _ := ls.find(idz)
    
    labelAA, _ := ls.find(idaa)
    labelBB, _ := ls.find(idbb)
    
    ls.rootNode().printTree("", "r", g)
    
    label1, _ := ls.find(uint16(1))
    fmt.Printf("1 bytes: %+v, 1 value: %s\n", label1.data(), label1.Value(g))
    fmt.Printf("Vertex < A %+v\n", "Vertex" < "A")
    
    if labelA != ls.findByValue("a", g) {
        fmt.Printf("A: %+v != %+v \n", labelA, ls.findByValue("a", g))
    }
    if labelB != ls.findByValue("b", g) {
        fmt.Printf("B: %+v != %+v \n", labelB, ls.findByValue("b", g))
    }
    if labelC != ls.findByValue("c", g){
        fmt.Printf("C: %+v != %+v \n", labelC, ls.findByValue("c", g))
    }
    if labelD != ls.findByValue("d", g){
        fmt.Printf("D: %+v != %+v \n", labelD, ls.findByValue("d", g))
    }
    if labelE != ls.findByValue("e", g){
        fmt.Printf("E: %+v != %+v \n", labelE, ls.findByValue("e", g))
    }
    if labelF != ls.findByValue("f", g){
        fmt.Printf("F: %+v != %+v \n", labelF, ls.findByValue("f", g))
    }
    
    fmt.Printf("A bytes: %+v \n", labelA.data())
    fmt.Printf("B bytes: %+v \n", labelB.data())
    fmt.Printf("C bytes: %+v \n", labelC.data())
    fmt.Printf("D bytes: %+v \n", labelD.data())
    fmt.Printf("E bytes: %+v \n", labelE.data())
    fmt.Printf("F bytes: %+v \n", labelF.data())
    
    fmt.Printf("X bytes: %+v \n", labelX.data())
    fmt.Printf("Y bytes: %+v \n", labelY.data())
    fmt.Printf("Z bytes: %+v \n", labelZ.data())
    fmt.Printf("AA bytes: %+v \n", labelAA.data())
    fmt.Printf("BB bytes: %+v \n", labelBB.data())
    
    fmt.Printf("A by value: %+v \n", ls.findByValue("a", g))
    fmt.Printf("B by value: %+v \n", ls.findByValue("b", g))
    fmt.Printf("C by value: %+v \n", ls.findByValue("c", g))
    fmt.Printf("D by value: %+v \n", ls.findByValue("d", g))
    fmt.Printf("E by value: %+v \n", ls.findByValue("e", g))
    fmt.Printf("F by value: %+v \n", ls.findByValue("f", g))
    fmt.Printf("G by value: %+v \n", ls.findByValue("g", g))
    
    fmt.Printf("Root: %+v \n", ls.rootNode())
    
    fmt.Printf("Writes: %+v \n", ls.writes)
    
    ls.write()
    
    fmt.Printf("Writes: %+v \n", ls.writes)
    
    fmt.Printf("Root: %+v \n", ls.rootNode())
    
    ts := g.textStore
    ts.write()
    
    labelA, _ = ls.find(ida)
    labelB, _ = ls.find(idb)
    labelC, _ = ls.find(idc)
    labelD, _ = ls.find(idd)
    labelE, _ = ls.find(ide)
    labelF, _ = ls.find(idf)
    
    fmt.Printf("A bytes: %+v \n", labelA.data())
    fmt.Printf("B bytes: %+v \n", labelB.data())
    fmt.Printf("C bytes: %+v \n", labelC.data())
    fmt.Printf("D bytes: %+v \n", labelD.data())
    fmt.Printf("E bytes: %+v \n", labelE.data())
    fmt.Printf("F bytes: %+v \n", labelF.data())
    
    fmt.Printf("A: %s", labelA.Value(g))
    fmt.Printf("B: %s", labelB.Value(g))
    fmt.Printf("C: %s", labelC.Value(g))
    fmt.Printf("D: %s", labelD.Value(g))
    fmt.Printf("E: %s", labelE.Value(g))
    fmt.Printf("F: %s", labelF.Value(g))
    
    
    
    // destroy the graph
    if err = db.DestroyGraph("test_graph"); err != nil {
        t.Error(err.Trace())
    }
    
    // destroy the database
    if err = db.Destroy(); err != nil {
        t.Error(err.Trace())
    }
    
}


