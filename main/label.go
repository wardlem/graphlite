package main

// Label is a struct representing an identifier entity.
// It is used for edge, attribute, and class labels.
type Label struct {
    id int16 // The id of the labels
    value string // The actual label value
    refs int64 // The number of times the label is used in the graph
}
