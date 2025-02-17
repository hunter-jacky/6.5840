package raft

import (
	"io/ioutil"
	"log"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

const (
	HEARTBEAT_INTERVAL  = 80 * time.Millisecond
	ELECTION_TIMEOUT    = 600 * time.Millisecond // 600-1000ms
	ELECTION_RAND_SCOPE = 400 * time.Millisecond
	VOTE_GRANT_TIMEOUT  = 400 * time.Millisecond // 400-600ms
	VOTE_RAND_SCOPE     = 200 * time.Millisecond
	SLEEP_INTERVAL      = 10 * time.Millisecond
)

type ElectionResult int

const (
	Win ElectionResult = iota
	Lose
	TimeOut
)

// 日志的相关设置
const log_output = true // 是否输出日志

// 日志的颜色设置
const (
	testFront    = "\033[31m" // 红色
	testEnd      = "\033[0m"
	warningFront = "\033[33m" // 黄色
	warningEnd   = "\033[0m"
)

// 红色的test日志，仅用于测试
var testPrintf = func(format string, a ...interface{}) {
	// 因为测试方法没有统一入口，无法一次性进行设置
	if log_output {
		log.Default().Printf(testFront+format+testEnd, a...)
	}
}

// 黄色的warning日志，不用于测试
var warningPrintf = func(format string, a ...interface{}) {
	log.Default().Printf(warningFront+format+warningEnd, a...)
}

// 普通日志，不用于测试
var logPrintf = func(format string, a ...interface{}) {
	log.Default().Printf(format, a...)
}

// 初始化日志设置
func initLogSetting() {
	// 时间戳精确到微秒
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds)
	// 设置是否输出日志
	if !log_output {
		log.SetOutput(ioutil.Discard)
	}
}
