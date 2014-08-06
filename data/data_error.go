package data

import (
	"github.com/wardlem/graphlite/util"
)

type DataError struct {
    Message string
    NextErr util.Error
    err error
}


func (e *DataError) Error() string {

    if (e.err != nil){
        return "Data Error: " + e.Message + "\n\t" + e.err.Error()
    }
    return "Data Error: " + e.Message;
} 

func dataError (message string, err error, next util.Error) *DataError {
    dataErr := DataError{message, next, err};
    return &dataErr
}

func (e *DataError) Trace() string {
    if e.Next() == nil || e.Next() == e{
        return e.Error()
    }
    return e.Error() + "\n" + e.Next().Trace()
}

func (e *DataError) Next() util.Error {
    return e.NextErr
}

func Assert(message string, assertions ...bool){
    util.Assert(message, assertions...)
}
