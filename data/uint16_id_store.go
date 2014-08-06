package data

import(
    "os"
    "io"
    "github.com/wardlem/graphlite/util"
)

type uint16IdStore struct {
    file *os.File
    lastId uint16 // The lastId that was used
    ids []uint16 // All ids that are available
}

func constructUint16IdStore(fileName string) (*uint16IdStore, *DataError) {
    store := new(uint16IdStore);
    if file, e := os.OpenFile(fileName, os.O_RDWR, 0777); (e != nil){
        return nil, dataError("Could not open file for attribute id store: " + fileName + ".", e, nil)
    } else {
        store.file = file
    }

    store.lastId, _ = store.readLastId()
    store.ids = store.readIds()
    
    return store, nil
}

func createUint16IdStore(fileName string) (*uint16IdStore, *DataError) {
    store := new(uint16IdStore);
    if file, e := os.OpenFile(fileName, os.O_RDWR | os.O_CREATE | os.O_EXCL, 0777); (e != nil){
        return nil, dataError("Could not open file for attribute id store: " + fileName + ".", e, nil)
    } else {
        store.file = file
    }

    store.writeLastId()
    
    return store, nil
}

func (store *uint16IdStore) write() {
    store.file.Truncate(int64(0))
    store.writeLastId()
    store.writeIds()
}

func (store *uint16IdStore) readLastId() (uint16, *DataError) {
    readAt := int64(0)
    b := make([]byte, 2)
    store.file.ReadAt(b, readAt)
    val, e := util.BytesToUint16(b)
    if (e != nil){
        return val, dataError("Unable to retrieve last id from id store.", e, nil)
    }
    return val, nil
}

func (store *uint16IdStore) writeLastId() {
    writeAt := int64(0)
    bytes, _ := util.Uint16ToBytes(store.lastId)
    _, _ = store.file.WriteAt(bytes, writeAt) // Ignore the return?
}

func (store *uint16IdStore) readIds() []uint16 {
    readAt := int64(2)
    b := make([]byte, 2)
    res := make([]uint16, 0)
    for _, e := store.file.ReadAt(b, readAt); e != io.EOF ; _, e = store.file.ReadAt(b, readAt){
        val, _ := util.BytesToUint16(b)
        res = append(res, val)
        readAt += 2
    }
    return res
}

func (store *uint16IdStore) writeIds() {
    var writeAt int64
    var b []byte 
    for idx, val := range store.ids {
        writeAt = int64(2 + 2 * idx)
        b, _ = util.Uint16ToBytes(val)
        _, _ = store.file.WriteAt(b, writeAt)
    }
}

func (store *uint16IdStore) nextId() uint16 {
    if len(store.ids) != 0 {
        id := store.ids[0]
        store.ids = store.ids[1:len(store.ids)]
        return id
    }
    store.lastId++;
    return store.lastId;
}

func (store *uint16IdStore) addId(id uint16) {
    store.ids = append(store.ids, id)
}

func (store *uint16IdStore) shutdown() {
    if store.file != nil {
        _ = store.file.Close()
    }

}


