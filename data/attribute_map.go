package data

// error messages
const (
    nilAttributeMap = "attempt to operate on nil attribute map"
)

type attributeMap map[string]*Attribute

func (m attributeMap) add (a *Attribute, g *Graph) *DataError {
    Assert(nilAttributeMap, m != nil)
    Assert(nilAttribute, a != nil)
    
    key, e := a.Key(g)
    if e == nil {
        m[key] = a
    }

    return e
}

func (m attributeMap) remove (a *Attribute, g *Graph) *DataError {
    Assert(nilAttributeMap, m != nil)
    Assert(nilAttribute, a != nil)
    
    key, e := a.Key(g)
    if e == nil {
        delete(m, key)
    }
    
    return e
}

func (m attributeMap) has (a *Attribute, g *Graph) bool {
    Assert(nilAttributeMap, m != nil)
    Assert(nilAttribute, a != nil)
    
    key, _ := a.Key(g)
    return m.hasKey(key)
}

func (m attributeMap) hasKey (key string) bool {
    Assert(nilAttributeMap, m != nil)
    
    _, ok := m[key]
    return ok
}

func (m attributeMap) get (key string) (*Attribute, bool) {
    a, ok := m[key]
    return a, ok
}

