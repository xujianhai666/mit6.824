package raft

import (
	"log"
	"sync/atomic"
)

//Debugging
var Debug = int32(0)
//var Debug = int32(1)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	//if Debug > 0 {
	if atomic.LoadInt32(&Debug) > 0 {
		log.Printf(format, a...)
	}
	return
}
