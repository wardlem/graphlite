package data

type MapItem struct {
    Attribute
}

func (attr *MapItem) NextId() uint32 {
    return attr.next
}

func (attr *MapItem) Next() (*MapItem, *DataError) {
    // Todo: get next item from store
    return nil, nil
}

type Map struct{
    items uint32
}

func constructMap(items uint32) *Map {
    m := new(Map);
    m.items = items
    return m
}




