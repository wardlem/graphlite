package data

type attributer interface {
    track(g *Graph)
}

// Attributable provides functionality for objects that can have attributes.
type attributable struct {
    firstAtt uint32
    aMap attributeMap // map stores the vertex's attributes by label
}

func (v *attributable) FirstAttribute(g *Graph) *Attribute {
	Assert(nilVertex, v != nil)
    Assert(nilGraph, g != nil)
	Assert(nilAttributeStore, g.attributeStore != nil)
	
	return g.attributeStore.Find(v.firstAtt)
}

// Returns a map of the attributes that belong to the object
// Operations on the returned map will have no effect on persistence
func (v *attributable) Attributes(g *Graph) attributeMap {
    Assert (nilVertex, v != nil)
    Assert (nilGraph, g != nil)
    
    if v.aMap == nil {
        m := make(attributeMap)
        a := v.FirstAttribute(g)
        for a != nil {
            m.add(a, g)
            a = a.Next(g)
        }
        
        v.aMap = m
    }
    
    return v.aMap
}

// Updates or adds an attribute to the vertex.
// This method takes care of tracking any changes to attributes or the vertex to the stores
func (v *attributable) SetAttribute(a *Attribute, g *Graph) {
    Assert (nilVertex, v != nil)
    Assert (nilGraph, g != nil)
    Assert (nilAttributeStore, g.attributeStore != nil)
    
    m := v.Attributes(g)
    key, _ := a.Key(g)
    
    if m.hasKey(key) {
        // update the existing attribute
        currentA, _ := m.get(key)
        currentA.t = a.t
        currentA.data = a.data
        g.attributeStore.Track(currentA)
    } else {
        // save the new attribute
        a.Id = g.attributeStore.nextId()
        a.next = v.firstAtt
        v.firstAtt = a.Id
        m.add(a, g)
        g.attributeStore.Track(a)
        v.track(g)
    }
}

func (v *attributable) RemoveAttribute(a *Attribute, g *Graph) {
    Assert(nilVertex, v != nil)
    Assert(nilGraph, g != nil)
    Assert(nilAttributeStore, g.attributeStore != nil)
    Assert(nilVertexStore, g.vertexStore != nil)
    
    m := v.Attributes(g)
    
    if ! m.has(a, g) {    // nothing to remove
        return
    }
    
    if v.firstAtt == a.Id {
        v.firstAtt = a.next
        v.track(g)
        g.attributeStore.Remove(a)
        m.remove(a, g)
        return
    }
    
    for _, attr := range m {
        if attr.next == a.Id {
            attr.next = a.next
            g.attributeStore.Track(attr)
            g.attributeStore.Remove(a)
            m.remove(a, g)
            break
        }
    }
}

func (v *attributable) RemoveAttributeByKey(key string, g *Graph) {
    Assert(nilVertex, v != nil)
    
    m := v.Attributes(g)
    a, ok := m.get(key)
    if ok {
        v.RemoveAttribute(a, g)
    }
}

func (v *attributable) track(g *Graph) {
    panic("attributable.track() should be overwritten")
}


