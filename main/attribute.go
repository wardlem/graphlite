package main

type Attribute struct {
	graph *Graph
	id    int32
	label Any     // int16 or *Label
	t     byte    // The type code of the attribute
	data  [8]byte // The raw data for the value of the attribute
	value Any     // The actual value of the attribute
}


