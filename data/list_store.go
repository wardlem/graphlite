package data

import (
    "os"
)

type listStore struct {
    file *os.File
    idStore *uint32IdStore
}

func constructListStore(g *Graph) (*listStore, *DataError){
    store := new(listStore)
    return store, nil
}

func createListStore(g *Graph) (*listStore, *DataError){
    store := new(listStore)
    return store, nil
}

func (store *listStore) shutdown () {
    if (store.idStore != nil){
        store.idStore.shutdown()
    }
    if (store.file != nil){
        _ = store.file.Close()
    }
}
