package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func Assert(expr bool, format string, a ...interface{}) {
	if Debug {
		if !expr {
			log.Fatalf(format, a...)
		}
	}
}
