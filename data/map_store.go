package data

import (
    "os"
)

type mapStore struct {
    file *os.File
    idStore *uint32IdStore
}

func constructMapStore(g *Graph) (*mapStore, *DataError){
    store := new(mapStore)
    return store, nil
}

func createMapStore(g *Graph) (*mapStore, *DataError){
    store := new(mapStore)
    return store, nil
}

func (store *mapStore) shutdown () {
    if (store.idStore != nil){
        store.idStore.shutdown()
    }
    if (store.file != nil){
        _ = store.file.Close()
    }
}
