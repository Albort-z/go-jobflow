// Package jobflow 该包实现了实现了一个可以异步处理的工作结构，其工作方法可自定义。
// 该JobFlowFunc类实现了一个多job流式处理的方法。
package jobflow

import (
	"errors"
)

// 流处理控制，需要实现用于支持可视化的status查看接口

// JobFlowFunc 工作流执行方法，一个流需要多个节点。是一种特殊的job
type JobFlowFunc struct {
	outputCh chan Output
	jobs     []*Job // 一个工作流中包含多个作业
}

var NoNeedErr = errors.New("数量少于2个不需要JobFlow")

func NewJobFlowFunc(jobs []*Job) (*JobFlowFunc, error) {
	if len(jobs) < 2 {
		return nil, NoNeedErr
	}
	var f = &JobFlowFunc{
		outputCh: make(chan Output),
		jobs:     jobs,
	}
	// 起多个协程协调处理结果的流转
	f.init()
	return f, nil
}

func (f *JobFlowFunc) Do(input Input) {
	firstJob := f.jobs[0]
	firstJob.Input(input)
}

func (f *JobFlowFunc) GetResult() (output Output) {
	return <-f.outputCh
}

// Init 起多个协程，将上一个处理的结果扔给下一个
func (f *JobFlowFunc) init() {
	// 最后一个job的输出不用处理，它会被GetResult消费
	var nextJob *Job
	for i, _ := range f.jobs {
		if i+1 < len(f.jobs) {
			nextJob = f.jobs[i+1]
		} else {
			nextJob = nil
		}
		go func(job0, job1 *Job) {
			for output := range job0.OutputCh() {
				if output.Error != nil || job1 == nil { // 执行失败或者最后一个任务的结果直接丢给输出通道
					f.outputCh <- output
				} else { // 中间结果往下个传
					input := Input{
						ID:    output.ID,
						Input: output.Output,
					}
					// 将处理结果输入到下一个任务
					job1.Input(input)
				}
			}
		}(f.jobs[i], nextJob)
	}
}
