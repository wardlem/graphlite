package data

import (
    "os"
    "fmt"
)

// error messages
const (
    nilClassStore = "attempt to operate on nil class store"
)

type classStore struct {
    file *os.File
    classes []*Class
}

func constructClassStore(g *Graph) (*classStore, *DataError){
    Assert(nilGraph, g != nil)
    store := new(classStore)
    fileName := g.storePath("class")
    
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); e != nil {
        return nil, dataError("Could not open file for class store: " + fileName + ".", e, nil)
    } else {
        store.file = file;
    }
    
    if classes, e := store.readClasses(); e != nil {
        return nil, dataError("Failure to construct class store.", nil, e)
    } else {
        store.classes = classes
    }
    
    return store, nil
}

func createClassStore(g *Graph) (*classStore, *DataError){
    Assert(nilGraph, g != nil)
    store := new(classStore)
    
    fileName := g.storePath("class")
    
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); e != nil {
        return nil, dataError("Could not create class store: " + fileName + ".", e, nil)
    } else {
        store.file = file;
    }
    
    if classes, e := store.readClasses(); e != nil {
        return nil, dataError("Failure to create class store.", nil, e)
    } else {
        store.classes = classes
    }
    
    return store, nil
}

func (s *classStore) initialize(g *Graph) {
    s.AddClass("Vertex", nil, g)
    s.write()
}

func (s *classStore) AddClass(name string, super *Class, g *Graph) uint8 {
    
    id := s.nextId()
    label := g.labelStore.addLabel(name, g)
    c := createClass(id, label, super, g)
    
    if int(c.Id) > len(s.classes){
        s.classes = append(s.classes, c)
    } else {
        s.classes[c.Id - 1] = c
    }
    return c.Id
}

func (s *classStore) Find(id uint8) *Class {
    if (id == 0){
        return nil
    }
    return s.classes[id - 1]
}

func (s *classStore) FindByName(name string, g *Graph) *Class {
    for _, class := range s.classes {
        className, _ := class.Name(g)
        if className == name {
            return class
        }
    }
    return nil
}

func (s *classStore) write () {
    s.file.Truncate(int64(0))
    for id, class := range s.classes {
        s.file.WriteAt(class.Data(), int64(id * 8))
    }
    for _, class := range s.classes {
        if class.index != nil {
            class.index.write()
        }
    }
}

func (s *classStore) readClasses() ([]*Class, *DataError ){
    
    offset := int64(0)
    bytes := make([]byte, classDataSize)
    info, _ := os.Stat(s.file.Name())
    size := info.Size()
    classes := make([]*Class, size/classDataSize)
    
    for offset < size {
        _, _ = s.file.ReadAt(bytes, offset)
        if class, e := constructClass(uint8(offset / classDataSize + 1), bytes); e != nil {
            return nil, dataError("Unable to load classes from data file.", nil, e)
        } else {
            classes = append(classes, class)
        }

        offset += classDataSize
    }
    
    return classes, nil
    
}


func (s *classStore) nextId() uint8 {
    if (s.classes == nil){
        panic("Class store has a nil class store.")
    } else {
        fmt.Println("s.classes is NOT nil")
    }
    for _, val := range s.classes {
        
        if val.label == 0 {
            return val.Id
        }
    }
    return uint8(len(s.classes)) + 1
}

func (store *classStore) shutdown () {
    if (store.file != nil){
        _ = store.file.Close()
    }
}
