package data

import (
    "os"
    "io"

    "github.com/wardlem/graphlite/util"
)

// error messages
const (
    nilLabelStore = "attempt to operate on a nil label store"
    zeroLabelId = "attempt to retrieve label with id of 0"
    nilLabelWriteMap = "attempt to operate on a nil label write map"
    nilLabelIdStore = "attempt to operate on a nil label id store"
    nilLabelStoreFile = "attempt to operate on a nil label store file"
)

const labelStoreHeaderSize = 2 // The number of bytes the label store uses for storing information

// The label store is responsible for the management of all text labels in a graph.
type labelStore struct {
    file *os.File
    idStore *uint16IdStore
    writes map[uint16]*Label
    root uint16
}


// Creates an existing label store.
// Panics if the graph parameter is nil.
// Returns an error of type *DataError if there are any problems opening files.
func constructLabelStore(g *Graph) (*labelStore, *DataError){
    Assert(nilGraph, g != nil)
    
    s := new(labelStore)
    
    // open the file for the label store
    fileName := g.storePath("label")
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); (e != nil) {
        return nil, dataError("Could not open file for attribute store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    // construct the id store for the label store
    fileName = g.storePath("label.id")    
    idStore, de := constructUint16IdStore(fileName)
    if (de != nil){
        return nil, de
    }
    s.idStore = idStore
    
    // initialize the write map
    s.writes = make(map[uint16]*Label)
    
    // retrieve any additional necessary information from the file
    s.readHeader()

    return s, nil
}

// Creates a new label store along with all necessary files.
// Panics if the graph parameter is nil.
// Returns an error of type *DataError if there are any problems creating files.
func createLabelStore(g *Graph) (*labelStore, *DataError){
    
    Assert(nilGraph, g != nil)
    
    s := new(labelStore)
    
    // create the file for the label store
    fileName := g.storePath("label")
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); (e != nil){
        return nil, dataError("Could not create file for attribute store: " + fileName + ".", e, nil)
    } else {
        s.file = file;
    }

    // create the id store for the label store
    fileName = g.storePath("label.id")
    if idStore, de := createUint16IdStore(fileName); (de != nil){
        return nil, de
    } else {
        s.idStore = idStore
    }
    
    // initialize the write map
    s.writes = make(map[uint16]*Label)
    
    // initialize any additional necessary values
    s.writeHeader()
    
    return s, nil;
}

func (s *labelStore) write () {
    Assert(nilLabelStore, s != nil)
    Assert(nilLabelStoreFile, s.file != nil)
    Assert(nilLabelIdStore, s.idStore != nil)
    Assert(nilLabelWriteMap, s.writes != nil)
    
    // write values that need to be written
    for _, label := range s.writes {
        data := label.data()
        writeAt := int64(labelStoreHeaderSize + (label.Id - 1) * labelDataSize)
        s.file.WriteAt(data, writeAt)
    }
    
    // write the header
    s.writeHeader()
    
    // reset the write map
    s.writes = make(map[uint16]*Label)
}

// Internal method used by the label store to read its header.
func (s *labelStore) readHeader () {
    Assert (nilLabelStore, s != nil)
    Assert (nilLabelStoreFile, s.file != nil)
    
    // Read the root id for the tree
    readAt := int64(0)
    root := make([]byte, 2)
    _, _ = s.file.ReadAt(root, readAt) // TODO don't ignore this error
    s.root, _ = util.BytesToUint16(root) // TODO don't ignore this error
}

// Internal method used by the label store to write its header.
func (s *labelStore) writeHeader () {
    Assert (nilLabelStore, s != nil)
    Assert (nilLabelStoreFile, s.file != nil)
    
    // Write the root id for the tree
    writeAt := int64(0)
    bytes, _ := util.Uint16ToBytes(s.root) // TODO don't ignore this error
    _, _ = s.file.WriteAt(bytes, writeAt) // TODO don't ignore this error
}

// Retrieves a label by id from the label store.
// Panics if the id is 0.
// Panics if the label store or the label store's write map are not initialized.
// May return an error of type *DataError

func (s *labelStore) find(id uint16) (*Label, *DataError) {
    Assert(nilLabelStore, s != nil)
    Assert(zeroLabelId, id != 0)
    Assert(nilLabelWriteMap, s.writes != nil)
    
    
    // see if the label is in the write map
    // this allows unwritten labels to be returned
    if l, ok := s.writes[id]; ok {
        return l, nil
    }
    
    // TODO implement internal caching of labels or read every time?
    
    // read the label from the file and return it
    readAt := int64(labelStoreHeaderSize + (id - 1) * labelDataSize)
    bytes := make([]byte, labelDataSize)
    c, e := s.file.ReadAt(bytes, readAt)
    if (e != nil && e != io.EOF) || c != labelDataSize {
        return nil, dataError("could not find label", e, nil)
    }
    return constructLabel(id, bytes)
}

// Internal function used to bypass zero id error for label lookup.
// Used primarily by the findByValue() method.
func (s *labelStore) findAllowZero(id uint16) (*Label, *DataError) {
    if id == 0 {
        return nil, nil
    }
    return s.find(id)
}


// Performs a search for the label.
// Returns the label if it is found, or nil if it is not found.
// Parameter value is the value searched for.
// Parameter g is the graph the label store belongs to.
func (s *labelStore) findByValue(value string, g *Graph) *Label {
    Assert(nilLabelStore, s != nil)
    Assert(nilLabelStoreFile, s.file != nil)
    Assert(nilGraph, g != nil)
    
    root, _ := s.findAllowZero(s.root) // TODO don't ignore this error
    
    // Search for the label using binary search
    for root != nil {
        rootVal := root.Value(g)
        if rootVal == value {
            return root     // this is the label we are looking for
        } else if rootVal < value {  // the exact method of comparison does not matter, so long
                                     // as it is consistent and deterministic
            root, _ = root.right(g) // go right on the tree
                                    // TODO do not ignore error
        } else {
            root, _ = root.left(g) // go left on the tree
                                    // TODO do not ignore error
        }
    }
    
    // TODO create index and retrieve that way
    return nil  // the value was not found
                // TODO return a new label if the label wasn't found?
}

// Adds a label to the store by string value.
// Actually increments the refs value of the label if it already exists,
// or creates it if it does not exist.
// Returns the id of the label.
func (s *labelStore) addLabel(value string, g *Graph) uint16 {
    Assert(nilLabelStore, s != nil)
    Assert(nilLabelWriteMap, s.writes != nil)
    Assert(nilLabelIdStore, s.idStore != nil)
    
    // search for an existing label
    l := s.findByValue(value, g);
    
    if l == nil {
        // create a new label
        l = newLabel(value, g)
        l.Id = s.idStore.nextId()
        
        // make sure we remember to write the label
        s.writes[l.Id] = l
        
        // restructure the tree
        root, _ := s.findAllowZero(s.root) // TODO don't ignore this error
        s.root = root.addNode(l, g)
    } else {
        // make sure we remember to write the label
        s.writes[l.Id] = l
    }
    
    // increment the reference count
    l.refs += 1
    
    
    
    return l.Id
}

func (s *labelStore) removeLabel(value string, g *Graph) {
    Assert(nilLabelStore, s != nil)
    Assert(nilLabelIdStore, s.idStore != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelWriteMap, s.writes != nil)
    
    l := s.findByValue(value, g)
    if (l != nil && l.Id != 0) {
        l.refs -= uint64(1);
        if l.refs <= uint64(0) {    // no more references
            s.deleteLabel(l, g)
        }
        s.writes[l.Id] = l
    }
    
}

// Internal function to make sure a label is deleted properly.
func (s *labelStore) deleteLabel(l *Label, g *Graph) {
    Assert(nilLabelStore, s != nil)
    Assert(nilLabelIdStore, s.idStore != nil)
    Assert(nilGraph, g != nil)
    Assert(nilTextStore, g.textStore != nil)
    
    if (l.Id == 0){  // TODO should this be an error?
        return
    }
    
    s.idStore.addId(l.Id)
    root, _ := s.find(s.root) // TODO do not ignore error
    
    // remove it from the tree
    s.root = root.removeNode(l, g)
    
    // let the text store get rid of the text
    t, _ := g.textStore.find(l.value)   // TODO safe to ignore?
    g.textStore.removeText(t)

}

// Shuts the label store down, making sure all files are closed.
func (s *labelStore) shutdown () {
    if (s != nil){
        if (s.idStore != nil){
            s.idStore.shutdown()
        }
        if (s.file != nil){
            _ = s.file.Close()
        }
    }
}

func (s *labelStore) rootNode () *Label {
    r, _ := s.find(s.root)
    return r
}
