package node

// Task 任务
type Task struct {
	UUID  string
	Input any
}

// TaskOutput 任务返回
type TaskOutput Task

// ErrOutput 失败返回
type ErrOutput struct {
	Outputs []any
	Err     error
}
