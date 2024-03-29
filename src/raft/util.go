package raft

import (
	"log"
	"math/rand"
	"time"
)

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

func Min(a int, b int) int {
	if b <= a {
		return b
	} else {
		return a
	}
}

func GetRandomTimeout(timeout time.Duration) time.Duration {
	return timeout + time.Duration(rand.Float64()*float64(timeout))
}
