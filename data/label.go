package data

import(
    "fmt"

    "github.com/wardlem/graphlite/util"
)

// Error strings
const (
    labelCreationFailure = "could not create label"
    labelWrongDataSize = "wrong data size when creating label"
    nilLabel = "attempt to operate on a nil label"
)

const labelDataSize = 21    // this is the number of bytes a label takes up in the file

// A label is any text value that is expected to be reused throughout the graph.
// This can be an edge label, an attribute name, a class name, a map key, etc.
type Label struct {
    Id uint16    // the id of the label
    value uint64 // the id of the text object for the label
    refs uint64  // the number of objects that use this label
                 // when refs < 1, it is deleted upon writing
    l uint16  // the left node for binary (avl) searches
    r uint16 // the right node for binary (avl) searches
    h uint8 // keeps track of the height of the node for avl tree operations
}

// Creates an existing label from byte data.
// Panics if the length of the bytes does not match labelDataSize.
// Returns an error of type *DataError if there is a conversion error.
func constructLabel (id uint16, bytes []byte) (*Label, *DataError) {
    Assert(labelWrongDataSize, len(bytes) == labelDataSize)
    
    var e error
    
    l := new(Label)
    l.Id = id
    
    val := bytes[0:8]
    refs := bytes[8:16]
    left := bytes[16:18]
    right := bytes[18:20]
    height := bytes[20]
    

    l.value, e = util.BytesToUint64(val)
    if e != nil {
        return nil, dataError(labelCreationFailure, e, nil)
    }
    l.refs, e = util.BytesToUint64(refs) 
    if e != nil {
        return nil, dataError(labelCreationFailure, e, nil)
    }
    if l.l, e = util.BytesToUint16(left); e != nil {
        return nil, dataError(labelCreationFailure, e, nil)
    }
    if l.r, e = util.BytesToUint16(right); e != nil {
        return nil, dataError(labelCreationFailure, e, nil)
    }
    l.h = height
    
    return l, nil
}

// Responsible for creating a new label.
// This method should only be called by the label store.
// The addLabel(value string) method of the label store should be called to create a label.
// This method panics if the graph is nil or the graph's text store is nil.
func newLabel(value string, g *Graph) *Label {
    Assert(nilGraph, g != nil)
    Assert(nilTextStore, g.textStore != nil)
    
    l := new(Label)
    t := newText(value)
    l.value = g.textStore.addText(t)
    
    return l
}

// Returns the string value of the label.
func (l *Label) Value(g *Graph) string {
    Assert(nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilTextStore, g.textStore != nil)
    
    t, _ := g.textStore.find(l.value) // TODO don't ignore error
    return t.Value()
}

// Returns the byte representation of the label for storage.
func (l *Label) data() []byte {
    Assert(nilLabel, l != nil)
    
    bytes, _ := util.Uint64ToBytes(l.value)
    refs, _ := util.Uint64ToBytes(l.refs)
    left, _ := util.Uint16ToBytes(l.l)
    right, _ := util.Uint16ToBytes(l.r)
    
    bytes = append(bytes, append(refs, append(left, append(right, l.h)...)...)...) // TODO ??
    return bytes
    
}

// Returns the height of a label for binary search operations.
func (l *Label) height() int8 {
    if (l == nil) {
        return int8(-1)
    }
    return int8(l.h)
}

// Calculates and sets the height of a label.
// Returns true if the height is changed, or false if it has not.
func (l *Label) setHeight(g *Graph) bool {
    // keep track of whether or not the height has changed
    changed := false
    
    Assert (nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    
    var newHeight uint8
    
    // get the heights of the right and left nodes
    left, _ := l.left(g) // TODO do not ignore this error
    right, _ := l.right(g) // TODO do not ignore this error
    
    lh := left.height()  
    rh := right.height() 
    
    // determine the new height
    if (lh < rh) {
        newHeight = uint8(1 + rh)
    } else {
        newHeight = uint8(1 + lh)
    }
    
    if int8(newHeight) != l.height() {
        changed = true
    }
    
    l.h = newHeight
    
    return changed
}

// Returns the right node for the label in the binary search tree.
func (l *Label) right(g *Graph) (*Label, *DataError) {
    Assert (nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    
    return g.labelStore.findAllowZero(l.r)
}

// Returns the left node for the label in the binary search tree.
func (l *Label) left(g *Graph) (*Label, *DataError) {
    Assert(nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    
    return g.labelStore.findAllowZero(l.l)
}

// Recursive function that adds a node to the binary search tree
func (currentLabel *Label) addNode(newLabel *Label, g *Graph) uint16 {
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    Assert(nilLabel, newLabel != nil)
    
    if currentLabel == nil {
        return newLabel.Id
    }
    
    currentVal := currentLabel.Value(g)
    newVal := newLabel.Value(g)
    
    if newVal < currentVal {
        ln, _ := currentLabel.left(g) // TODO do not ignore error
        l := ln.addNode(newLabel, g)
        if l != currentLabel.l {
            g.labelStore.writes[currentLabel.Id] = currentLabel
        }
        currentLabel.l = l
    } else {
        rn, _ := currentLabel.right(g) // TODO do not ignore error
        r := rn.addNode(newLabel, g) 
        if r != currentLabel.r {
            g.labelStore.writes[currentLabel.Id] = currentLabel
        }
        currentLabel.r = r
    }
    
    currentLabel.setHeight(g)
    return currentLabel.balance(g)
}

// Updates the the structure of the label avl tree when removing a label.
func (currentLabel *Label) removeNode(removeLabel *Label, g *Graph) uint16 {
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    Assert(nilLabel, removeLabel != nil)
    
    // make sure we haven't reached the end of the road
    if (currentLabel == nil) {  // TODO should this cause an error?
        return uint16(0)
    }
    
    // this is the one we want
    if removeLabel.Id == currentLabel.Id {
        // remove this label
        cl, _ := currentLabel.left(g) // TODO do not ignore error
        cr, _ := currentLabel.right(g) // TODO do not ignore error
        
        if cl == nil && cr == nil { // no descendents
            return uint16(0)
        } else if cl == nil { // one descendent
            return cr.Id
        } else if cr == nil { // one descendent
            return cl.Id
        } else if cl.height() > cr.height() {
            // get the right most node of the left branch
            rLabel := cl.rightmostNode(g)
            rLabel.l = cl.removeNode(rLabel, g)
            rLabel.r = currentLabel.r
            g.labelStore.writes[rLabel.Id] = rLabel
            return rLabel.balance(g)
        } else {
            // get the left most node of the right branch
            lLabel := cr.leftmostNode(g)
            lLabel.r = cl.removeNode(lLabel, g)
            lLabel.l = currentLabel.l
            g.labelStore.writes[lLabel.Id] = lLabel
            return lLabel.balance(g)
        }
       
    // keep looking
    } else if removeLabel.Value(g) < currentLabel.Value(g) {
        left, _ := currentLabel.left(g) // TODO do not ignore error
        l := left.removeNode(removeLabel, g)
        if (l != currentLabel.l) {
            g.labelStore.writes[currentLabel.Id] = currentLabel
        }
        currentLabel.l = l
    } else {
        right, _ := currentLabel.right(g) // TODO do not ignore error
        r := right.removeNode(removeLabel, g)
        if (r != currentLabel.r) {
            g.labelStore.writes[currentLabel.Id] = currentLabel
        }
        currentLabel.r = r
    }
    
    return currentLabel.balance(g)
    
}

// Balances the avl tree.
func (l *Label) balance(g *Graph) uint16 {
    Assert(nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    Assert(nilLabelWriteMap, g.labelStore.writes != nil)
    
    // set the height
    changed := l.setHeight(g) 
    if changed {
        g.labelStore.writes[l.Id] = l
    }
    
    // get the current balance
    b := l.currentBalance(g)
    if b < -1 {
        // make sure we remember to write the changes
        g.labelStore.writes[l.Id] = l
        left, _ := l.left(g)    // TODO do not ignore error
        if left.currentBalance(g) > 0 {  // double rotation
            l.l = left.rotateLeft(g)
        }
        return l.rotateRight(g)
    } else if b > 1 {
        // make sure we remember to write the changes
        g.labelStore.writes[l.Id] = l
        right, _ := l.right(g) // TODO do not ignore error
        if right.currentBalance(g) < 0 { // double rotation
            l.r = right.rotateRight(g)
        }
        return l.rotateLeft(g)
    }
    
    return l.Id
}

// Returns the current balance of the label for avl operations.
func (l *Label) currentBalance(g *Graph) int {
    Assert(nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    
    right, _ := l.right(g) // TODO do not ignore the error
    left, _ := l.left(g) // TODO do not ignore the error
    rh := right.height()
    lh := left.height()
    return int(rh) - int(lh)
}

// Rotates the label right to balance the label avl tree.
func (l *Label) rotateRight(g *Graph) uint16 {
    Assert(nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    Assert(nilLabelWriteMap, g.labelStore.writes != nil)
    
    // perform the rotation
    left, _ := l.left(g) // TODO do not ignore error
    l.l = left.r
    left.r = l.Id
    

    l.setHeight(g)
    left.setHeight(g)
    
    // make sure the changes are written
    g.labelStore.writes[l.Id] = l
    g.labelStore.writes[left.Id] = left
    
    return left.Id
}

// Rotates the label left to balance the label avl tree
func (l *Label) rotateLeft(g *Graph) uint16 {
    Assert(nilLabel, l != nil)
    Assert(nilGraph, g != nil)
    Assert(nilLabelStore, g.labelStore != nil)
    Assert(nilLabelWriteMap, g.labelStore.writes != nil)
    
    // perform the rotation
    right, _ := l.right(g) // TODO do not ignore error
    l.r = right.l
    right.l = l.Id
    
    l.setHeight(g)
    right.setHeight(g)
    
    // make sure the changes are written
    g.labelStore.writes[l.Id] = l
    g.labelStore.writes[right.Id] = right
    
    return right.Id
}

// Recursive function that returns the leftmost node of a label
func (l *Label) leftmostNode(g *Graph) *Label {
    left, _ := l.left(g) // TODO do not ignore error
    if left == nil {
        return l
    }
    return left.leftmostNode(g)
}

// Recursive function that returns the rightmost node of a label
func (l *Label) rightmostNode(g *Graph) *Label {
    right, _ := l.right(g) // TODO do not ignore error
    if right == nil {
        return l
    }
    return right.rightmostNode(g)
}

func (l *Label) printTree(spaces string, side string, g *Graph) {
    if (l == nil) {
        fmt.Printf("%s(%s)<nil>\n", spaces, side)
    } else {
        fmt.Printf("%s(%s)%s: %d\n", spaces, side, l.Value(g), l.height())
        spaces += "  "
        left, _ := l.left(g)
        right, _ := l.right(g)
        left.printTree(spaces, "l", g)
        right.printTree(spaces, "r", g)
    }
}
