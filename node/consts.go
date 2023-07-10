package node

import "errors"

type status int

const (
	_ status = iota
	StartingStatus
	StartedStatus
	StoppingStatus // 停止中
	StoppedStatus  // 已停止
	PausedStatus   // 已暂停
)

var statusNameMap = map[status]string{
	StartingStatus: "启动中",
	StartedStatus:  "已启动",
	StoppingStatus: "停止中",
	StoppedStatus:  "已停止",
	PausedStatus:   "已暂停",
}

var (
	IsStoppedErr   = errors.New("node已停止")
	TransitionsErr = errors.New("node正在更改")
)
