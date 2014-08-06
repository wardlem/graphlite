package data

import (
    "os"    // file operations
    "io"
    
    "github.com/wardlem/graphlite/util" // type conversions
)

// Error messages
const(
    openTextIdFileFail = "could not open file for text id store: "
    createTextIdFileFail = "could not create file for text id store: "
    nilTextIdStore = "attempt to operate on nil text id store"
    nilTextIdStoreFile = "attempt to operate on nil text id store file"
    nilTextIdStoreSlice = "attempt to operate on nil text id store slice"
)

// The text id store is responsible for keeping track of what ids are available
// for the text store to use.
type textIdStore struct {
    file *os.File  // file for the id store
    next uint64  // the next id to use if no others are available
    ids []*textId  // the available ids for the store
}

// Responsible for constructing a text id store that already exists.
// An error of type *DataError is returned if the file can not be opened.
func constructTextIdStore(fileName string) (*textIdStore, *DataError) {
    s := new(textIdStore);
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); (e != nil){
        return nil, dataError(openTextIdFileFail + fileName, e, nil)
    } else {
        s.file = file
    }

    s.next, _ = s.readNextId()
    s.ids = s.readIds()
    
    return s, nil
}

// Responsible for creating a text id store that does not yet exist.
// An error of type *DataError is returned if the file can not be created.
func createTextIdStore(fileName string) (*textIdStore, *DataError) {
    s := new(textIdStore);
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); (e != nil){
        return nil, dataError(createTextIdFileFail + fileName, e, nil)
    } else {
        s.file = file
    }

    s.next = uint64(1)    // ids start at 1
    s.writeNextId()
    s.ids = s.readIds()
    
    return s, nil
}

// Adds a text id object to the id store.
func (s *textIdStore) addId(id *textId) {
    Assert(nilTextIdStore, s != nil)
    Assert(nilTextIdStoreSlice, s.ids != nil)
    s.ids = append(s.ids, id)
}

// Writes the data for the store to the file.
func (s *textIdStore) write() {
    Assert(nilTextIdStore, s != nil)
    Assert(nilTextIdStoreFile, s.file != nil)
    
    s.file.Truncate(int64(0))
    s.writeNextId()
    s.writeIds()
}

// Reads the final available id (used when no other ids can be used) from the data file.
// Returns an error of type *DataError when there is a data conversion failure.
func (s *textIdStore) readNextId() (uint64, *DataError) {
    Assert(nilTextIdStore, s != nil)
    Assert(nilTextIdStoreFile, s.file != nil)
    
    readAt := int64(0)
    b := make([]byte, 8)
    s.file.ReadAt(b, readAt)
    val, e := util.BytesToUint64(b)
    if (e != nil){
        return val, dataError("Unable to retrieve next id from text id store.", e, nil)
    }
    return val, nil
}

// Writes the final available id (used when no other ids can be used) to the data file.
func (s *textIdStore) writeNextId() {
    writeAt := int64(0)
    bytes, _ := util.Uint64ToBytes(s.next)
    _, _ = s.file.WriteAt(bytes, writeAt) // TODO ignore the return?
}

// Reads the available ids from the data file and returns them.
func (s *textIdStore) readIds() []*textId {
    Assert(nilTextIdStore, s != nil)
    Assert(nilTextIdStoreFile, s.file != nil)
    
    readAt := int64(8)
    b := make([]byte, textIdDataSize)
    res := make([]*textId, 0)
    for _, e := s.file.ReadAt(b, readAt); e != io.EOF ; _, e = s.file.ReadAt(b, readAt){
        val, _ := constructTextId(b)
        res = append(res, val)
        readAt += textIdDataSize
    }
    return res
}

// Writes the available ids to the data file.
func (s *textIdStore) writeIds() {
    Assert(nilTextStore, s != nil)
    Assert(nilTextStoreFile, s.file != nil)
    
    var writeAt int64
    var b []byte 
    for idx, val := range s.ids {
        if val != nil && val.rows > 0 {     // ensure the value should/can be written
                                            // should nil cause a panic?
            writeAt = int64(8 + textIdDataSize * idx)
            b = val.data()
            _, _ = s.file.WriteAt(b, writeAt)   // TODO probably shouldn't ignore this
        }
        
    }
}

// Returns the next available id from the text id store.
// Size is the size in bytes of the text value (calculated using the Len() method
// of a text struct).
// The text id store must find an available id that also has a contiguous section
// of the file that will hold the new string.
// If no ids are available, the next id value of the id store will be used.
// Retrieving a next id alters the state of the id store to prevent duplicate ids.
// However, nothing is persisted until the text id store's write() method is called.
func (s *textIdStore) nextId(size uint32) (val uint64) {
    Assert(nilTextIdStore, s != nil)
    Assert(nilTextIdStoreSlice, s.ids != nil)

    // calculate the number of rows we need
    rows := calculateTextRows(size)
    
    // search for an existing id that is usable
    for _, id := range s.ids {
        if id.rows <= rows {
            val = id.value
            idRows := id.rows
            
            // update the id to reflect the space available
            id.rows = idRows - rows
            id.value = val + uint64(rows)
            
            return val
        }
    }
    
    // if no other id works, use the next id
    val = s.next
    s.next += uint64(rows)
    return val
}

// This is a utility function to calculate the number of rows that will be required
// to store a text object in the database.
// Size should be the size in bytes of the string value of the text object (
// this can be retrieved using the text objects Len() method).
func calculateTextRows(size uint32) uint32{
    size += 4 // added for the stored size value
    rows := size / textStoreRowSize
    if size % textStoreRowSize != 0 {   // Round up, not down
        rows += 1
    }
    return rows
}

func (s *textIdStore) shutdown() {
    if s != nil {
        if s.file != nil {
            _ = s.file.Close()
        }
    }
    
}
