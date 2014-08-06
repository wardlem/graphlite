package data

type ListItem struct {
    Attribute
}

func (*ListItem) Label() (*Label, *DataError) {
    return nil, nil
}

func (attr *ListItem) NextId() uint32 {
    return attr.next
}

func (attr *ListItem) Next() (*ListItem, *DataError) {
    // Todo: get next list from store
    return nil, nil
}

type List struct{
    items uint32 // id of the first list item.  use as a linked list.
}

func constructList(items uint32) *List {
    l := new(List);
    l.items = items
    return l
}

func (l *List) Items() (*ListItem, *DataError)  {
    return nil, nil
}
