package util

import (
    "fmt"
)

type Error interface {
    Error() string
    Trace() string
    Next() Error
}

type GLError struct{
    Message string
    NextErr Error
}

func (e *GLError) Trace () string {
    //if (e.Next() == nil){
        return "Graphlite Error: " + e.Error()
    //}
    //return "Graphlite Error: " + e.Error() + "\n" + e.Next().Trace()
}

func (e *GLError) Next() Error {
    return e.NextErr
}

func (e *GLError) Error() string {
    return e.Message
}

func Assert(message string, assertions ...bool){
    for pos, assert := range assertions {
        if (!assert) {
            panic(fmt.Sprintf("Failed assertion: %s (assertion #%d)", message, pos + 1))
        }
    }
}





