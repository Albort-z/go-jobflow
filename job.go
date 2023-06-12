// Package jobflow 该包实现了实现了一个可以异步处理的工作结构，其工作方法可自定义。
// 该Job类只有一个输入参数和一个输出参数，其输出中包含正常输出和异常原因。
// todo: 支持多节点并发(多节点时不保证处理输出列表顺序与输入一致)、支持存储进度与重启恢复、判断节点、进度可视化
package jobflow

import "sync/atomic"

type Job struct {
	inputCh  chan Input
	outputCh chan Output
	doingNum atomic.Int32
	goToStop atomic.Bool

	// Name 获取任务名称
	name string
	// Do 工作
	do Func
}

type Input struct {
	ID    string
	Input any
}

type Output struct {
	ID     string
	Output any
	Error  error
}

// Func Job节点的处理方法接口，定义如何处理输入，如何获取输出
type Func interface {
	Do(Input)
	GetResult() Output
}

// BaseFunc 定义结果返回函数
type BaseFunc struct {
	outputCh chan Output
}

func NewBaseFunc() *BaseFunc {
	return &BaseFunc{
		outputCh: make(chan Output),
	}
}

func (f *BaseFunc) SetResult(output Output) {
	f.outputCh <- output
}

func (f *BaseFunc) GetResult() Output {
	return <-f.outputCh
}

// RunJob 运行一台工作节点, do为实际的处理方法, pendingCap为等待队列的最大容量
func RunJob(name string, do Func, pendingCap int) *Job {
	var job = &Job{
		inputCh:  make(chan Input, pendingCap),
		outputCh: make(chan Output),
		name:     name,
		do:       do,
	}
	go job.run()
	return job
}

func (j *Job) run() {
	// 输入传输带
	go func() {
		for input := range j.inputCh {
			j.doingNum.Add(1)
			j.do.Do(input)
		}
	}()

	// 输出传输带
	go func() {
		defer j.stop()
		for {
			j.outputCh <- j.do.GetResult()
			j.doingNum.Add(-1)
			if j.goToStop.Load() && int(j.doingNum.Load())+len(j.inputCh) == 0 {
				return
			}
		}
	}()
}

// Input 输入任务, 停止后不能再输入
func (j *Job) Input(input Input) {
	j.inputCh <- input
	return
}

// OutputCh 输出通道, 直接返回只读channel，便于判断是否结束(当chan关闭时结束)
func (j *Job) OutputCh() <-chan Output {
	return j.outputCh
}

// Stop 停止工作
func (j *Job) Stop() {
	// 立即关闭输入通道
	close(j.inputCh)
	// 若数据全部处理完就直接停止，否则等待处理完成后停止
	if int(j.doingNum.Load())+len(j.inputCh) == 0 {
		j.stop()
	} else {
		j.goToStop.Store(true)
	}
}

// 释放资源，持久化等
func (j *Job) stop() {
	close(j.outputCh)
	// todo 存储pending队列
}

// PendingSize 待处理队列长度，不包含正在处理的
func (j *Job) PendingSize() int {
	return len(j.inputCh)
}
