package data

import (
    "os"
    
    "github.com/wardlem/graphlite/util"
)

// Error messages
const (
    nilClassIdIndex = "attempt to operate on a nil class id index"
    nilClassIdIndexFile = "attempt to operate on a nil class id index file"
)

// A class id index is responsible for indexing the vertices that belong to a class
type classIdIndex struct {
    file *os.File
    ids map[uint32]Empty
}

// Creates an existing class id index.
func constructClassIdIndex(fileName string) (*classIdIndex, *DataError) {
    
    i := new(classIdIndex)
    
    // load the file
    file, e := os.OpenFile(fileName, os.O_RDWR, 0777)
    if e != nil {
        return nil, dataError("Could not open file for class id index: " + fileName + ".", e, nil)
    }
    i.file = file;
    
    i.readIds()
    
    return i, nil
}

// Creates a class id index that does not yet exist.
func createClassIdIndex(fileName string) (*classIdIndex, *DataError) {
    
    i := new(classIdIndex)
    
    // create the file
    file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777)
    if e != nil {
        return nil, dataError("Could not create file for class id index: " + fileName + ".", e, nil)
    }
    i.file = file;
    
    i.readIds()
    
    return i, nil
}

// Reads the ids from the file and stores it in the ids map of the index
// Has a secondary purpose of initializing the indexes id map
func (i *classIdIndex) readIds () {
    Assert(nilClassIdIndex, i != nil)
    Assert(nilClassIdIndexFile, i.file != nil)
    
    ids := make(map[uint32]Empty)
    
    offset := int64(0)
    info, _ := os.Stat(i.file.Name())
    size := info.Size()
    bytes := make([]byte, size)
    _, _ = i.file.ReadAt(bytes, offset)
    
    pos := 0
    for int64(pos) < size {
        idBytes := bytes[pos : pos + 4]
        id, _ := util.BytesToUint32(idBytes)
        ids[id] = Empty{}
        pos += 4
    }
    
    i.ids = ids
}

// Writes the ids to the file.
func (i *classIdIndex) write () {
    Assert(nilClassIdIndex, i != nil)
    Assert(nilClassIdIndexFile, i.file != nil)
    
    i.file.Truncate(int64(0))
    
    ids := make([]byte,0 ,len(i.ids) * 4)
    for id, _ := range i.ids {
        bytes, _ := util.Uint32ToBytes(id)
        ids = append(ids, bytes...)
    }
    
    writeAt := int64(0)
    _, _ = i.file.WriteAt(ids, writeAt) // TODO do not ignore the error
    
}


// Determines if a particular id is present in the index.
func (i *classIdIndex) hasId(id uint32) bool {
    _, ok := i.ids[id]
    return ok
}

// Returns a set of all the ids in the class.
func (i *classIdIndex) allIds() map[uint32]Empty {
    return i.ids
}

func (i *classIdIndex) addId(id uint32) {
    i.ids[id] = Empty{}
}

func (i *classIdIndex) removeId(id uint32) {
    delete(i.ids, id)
}

// Cleans up the file for the index.
func (i *classIdIndex) shutdown() {
    if i != nil && i.file != nil {
        _ = i.file.Close()
    }
}


