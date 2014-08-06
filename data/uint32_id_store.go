package data

import(
    "os"
    "io"
    "github.com/wardlem/graphlite/util"
)

type uint32IdStore struct {
    file *os.File
    lastId uint32 // The lastId that was used
    ids []uint32 // All ids that are available
}

func constructUint32IdStore(fileName string) (*uint32IdStore, *DataError) {
    store := new(uint32IdStore);
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); (e != nil){
        return nil, dataError("Could not open file for attribute id store: " + fileName + ".", e, nil)
    } else {
        store.file = file
    }

    store.lastId, _ = store.readLastId()
    store.ids = store.readIds()
    
    return store, nil
}

func createUint32IdStore(fileName string) (*uint32IdStore, *DataError) {
    store := new(uint32IdStore);
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); (e != nil){
        return nil, dataError("Could not open file for attribute id store: " + fileName + ".", e, nil)
    } else {
        store.file = file
    }

    store.writeLastId()
    
    return store, nil
}

func (store *uint32IdStore) write() {
    store.file.Truncate(int64(0))
    store.writeLastId()
    store.writeIds()
}

func (store *uint32IdStore) readLastId() (uint32, *DataError) {
    readAt := int64(0)
    b := make([]byte, 4)
    store.file.ReadAt(b, readAt)
    val, e := util.BytesToUint32(b)
    if (e != nil){
        return val, dataError("Unable to retrieve last id from id store.", e, nil)
    }
    return val, nil
}

func (store *uint32IdStore) writeLastId() {
    writeAt := int64(0)
    bytes, _ := util.Uint32ToBytes(store.lastId)
    _, _ = store.file.WriteAt(bytes, writeAt) // Ignore the return?
}

func (store *uint32IdStore) readIds() []uint32 {
    readAt := int64(4)
    b := make([]byte, 4)
    res := make([]uint32, 0)
    for _, e := store.file.ReadAt(b, readAt); e != io.EOF ; _, e = store.file.ReadAt(b, readAt){
        val, _ := util.BytesToUint32(b)
        res = append(res, val)
        readAt += 4
    }
    return res
}

func (store *uint32IdStore) writeIds() {
    var writeAt int64
    var b []byte 
    for idx, val := range store.ids {
        writeAt = int64(4 + 4 * idx)
        b, _ = util.Uint32ToBytes(val)
        _, _ = store.file.WriteAt(b, writeAt)
    }
}

func (store *uint32IdStore) nextId() uint32 {
    if len(store.ids) != 0 {
        id := store.ids[0]
        store.ids = store.ids[1:len(store.ids)]
        return id
    }
    store.lastId++;
    return store.lastId;
}

func (store *uint32IdStore) addId(id uint32) {
    store.ids = append(store.ids, id)
}

func (store *uint32IdStore) shutdown() {
    if store.file != nil {
        _ = store.file.Close()
    }
}


