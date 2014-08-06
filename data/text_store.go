package data

import (
    "os"
    //"fmt"
    
    "github.com/wardlem/graphlite/util"
)

// error messages
const (
    nilTextStore = "attempt to operate on nil text store"
    textStoreFileOpenFail = "could not open file for text store: "
    textStoreFileCreateFail = "could not create file for text store: "
    nilTextStoreWriteSlice = "attempt to operate on a nil text store write slice"
    nilTextStoreFile = "attempt to operate on a nil text store file"
    zeroTextId = "attempt to retrieve text from store with id of zero"
)

const (
    textStoreRowSize = 16   // this is the size of a 'row' in the text store
                            // not sure what it should actually be
)

// The text store is responsible for managing the persistence and retrieval of text objects.
type textStore struct {
    file *os.File           // the file where the data is stored
    idStore *textIdStore    // stores unused ids for the text store
    writes []*Text          // remembers what it needs to write
}

// Creates an existing text store.
// Returns an error of type *DataError if the file can not be opened or
// if there is a problem creating the id store for the text store
func constructTextStore(g *Graph) (*textStore, *DataError){
    Assert(nilGraph, g != nil)
    
    s := new(textStore)
    
    // open the file
    fileName := g.storePath("text")
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); (e != nil) {
        return nil, dataError(textStoreFileOpenFail + fileName, e, nil)
    } else {
        s.file = file;
    }

    // create the id store
    fileName = g.storePath("text.id")
    idStore, de := constructTextIdStore(fileName)
    if (de != nil){
        return nil, de
    }
    s.idStore = idStore
    
    // initialize the write slice
    s.writes = make([]*Text, 0)
    
    return s, nil;
}

// Creates a new text store.
// Returns an error of type *DataError if the file can not be created or
// if there is a problem creating the id store for the text store.
func createTextStore(g *Graph) (*textStore, *DataError){
    Assert(nilGraph, g != nil)
    
    s := new(textStore)
    
    // create the file
    fileName := g.storePath("text")
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); (e != nil) {
        return nil, dataError(textStoreFileCreateFail + fileName, e, nil)
    } else {
        s.file = file;
    }

    // create the id store
    fileName = g.storePath("text.id")
    idStore, de := createTextIdStore(fileName)
    if (de != nil){
        return nil, de
    }
    s.idStore = idStore
    
    // initialize the write slice
    s.writes = make([]*Text, 0)
    
    return s, nil;
}

// Locates a text value by id and returns it.
func (s *textStore) find (id uint64) (*Text, *DataError) {
    Assert(nilTextIdStore, s != nil)
    Assert(zeroTextId, id != 0) // the id can not be zero
    Assert(nilTextStoreWriteSlice, s.writes != nil)
    Assert(nilTextStoreFile, s.file != nil)
    
    // search the writes for the text object
    for _, t := range s.writes {
        if t.Id == id {
            return t, nil
        }
    }
    
    // search for it in the file
    // first, get the size of the text
    readAt := int64((id - 1) * textStoreRowSize)
    sizeBytes := make([]byte, 4)
    _, _ = s.file.ReadAt(sizeBytes, readAt) // TODO this should NOT be ignored
    length, _ := util.BytesToUint32(sizeBytes) // TODO do not ignore error
    // then, read the actual text
    readAt += 4
    textBytes := make([]byte, length)
    _, _ = s.file.ReadAt(textBytes, readAt)
    // return a newly constructed text object
    
    return constructText(id, length, textBytes), nil
}

// Adds a text object to the text store.
// Returns the id that is determined for the text object
// Panics if the text store or text object is nil or if t already has an id
// Nothing is persisted until the text store's write() method is called
func (s *textStore) addText (t *Text) uint64 {
    Assert (nilTextStore, s != nil)
    Assert (nilText, t != nil)
    Assert ("cannot add text that already has an id", t.Id == 0)
    Assert (nilTextStoreWriteSlice, s.writes != nil)
    
    t.Id = s.idStore.nextId(t.Len())
    t.length = t.Len() // TODO necessary?
    
    // make sure the store remembers to write this
    s.writes = append(s.writes, t)

    return t.Id
}

// Removes a text object from the text store.
// The caller of this method is responsible for ensuring that there are no 
// more references to the text object.
func (s *textStore) removeText (t *Text) {
    Assert (nilTextStore, s != nil)
    Assert (nilText, t != nil)
    Assert (nilTextIdStore, s != nil)
    
    if (t.Id != 0){             // if the id is zero, it was never saved
    
        // give the id back to the id store so it can be recycled
        rows := calculateTextRows(t.length)
        id := newTextId(t.Id, rows)
        s.idStore.addId(id)
        
        t.Id = 0
        
        // TODO do we need to write it or do we just wait for it to be overwritten ??
        
    }
}

// Prepares a new or existing text object for persistence.
// Returns the id of the text object.
// Important: the id of the text object can be changed after calling this method.
// It is up to the caller of this method to ensure that references are updated properly.
func (s *textStore) saveText(t *Text) uint64 {
    Assert (nilTextStore, s != nil)
    Assert (nilTextIdStore, s.idStore != nil)
    Assert (nilTextStoreWriteSlice, s.writes != nil)
    
    // if the id is 0, it doesn't exist yet, so we add it
    if (t.Id == 0) {
        return s.addText(t)
    }
    
    // determine if we need a new id
    oldRows := calculateTextRows(t.length)
    newRows := calculateTextRows(t.Len())
    
    if (oldRows > newRows) {            // Keep the id, but recycle unused space
        idToCreate := t.Id + uint64(newRows)
        createRows := oldRows - newRows
        id := newTextId(idToCreate, createRows)
        s.idStore.addId(id)
    } else if (oldRows < newRows) {     // Needs a new id
        s.removeText(t)
        return s.addText(t)
    }
    
    // make sure the store remembers to write this
    s.writes = append(s.writes, t)
    
    // update the length of the text object
    t.length = t.Len()
    return t.Id
}

// Writes updates to the text store.
func (s *textStore) write() {
    Assert(nilTextStore, s != nil)
    Assert(nilTextIdStore, s.idStore != nil)
    Assert(nilTextStoreWriteSlice, s.writes != nil)
    Assert(nilTextStoreFile, s.file != nil)
    
    // write any new or updated values
    for _, t := range s.writes {
        Assert("can not write a text object with an id of 0", t.Id != 0)
        pos := int64(t.Id * textStoreRowSize - textStoreRowSize)
        // note: subtract textStoreRowSize is subtracted because ids begin at one, but writing starts at 0
        _, _ = s.file.WriteAt(t.data(), pos) // TODO probably shouldn't ignore this
    }
    
    // reset the write slice
    s.writes = make([]*Text, 0)
    
    // write the ids
    s.idStore.write()
}

func (s *textStore) shutdown () {
    if (s != nil){
        if (s.idStore != nil){
            s.idStore.shutdown()
        }
        if (s.file != nil){
            _ = s.file.Close()
        }
    }
    
}
