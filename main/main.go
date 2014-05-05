package main

import (
	"flag"
	//"fmt"
)

type Any interface{}

type Command func([]Any) Entity

type Entity interface{
    // Figure this out later
}

// main takes in command line arguments and runs them against the database.
func main() {
	flag.String("output", "JSON", "Determines the form of the output string.  Possible values: [JSON, PHP].")
	flag.String("use", "", "Determines which graph to use for the the session.")
	flag.Parse()
}



