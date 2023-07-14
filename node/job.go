package node

import (
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"go-jobflow/queue"
)

// Node 工作节点，若配置后续节点，则会自动传递给后续节点处理
type Node struct {
	name        string // 任务名称，在关闭时会将未完成的任务按该名称存储
	bufferSize  int    // 缓冲大小，即排队队列的最大长度
	concurrency int    // 并发数量

	current   string     // 当前正在处理的ID
	statusMtx sync.Mutex // 状态锁，在执行状态更改期间不接收其他更改命令
	status    status
	runWG     sync.WaitGroup
	costs     []time.Duration

	inputCh  *queue.Queue
	outputCh chan TaskOutput

	f Func

	nextNode *Node
	errNode  *Node
}

func NewNode(name string, bufferSize int, concurrency int, f Func) *Node {
	return &Node{
		name:        name,
		bufferSize:  bufferSize,
		concurrency: concurrency,
		status:      StoppedStatus,
		runWG:       sync.WaitGroup{},
		f:           f,
	}
}

func (j *Node) AppendJobs(nextJobs ...*Node) (err error) {
	curr := j
	for _, nextJob := range nextJobs {
		err = curr.appendJob(nextJob)
		if err != nil {
			return
		}
		curr = nextJob
	}
	return nil
}

// 在Job处理结果后添加后续处理Job, 判断类型
func (j *Node) appendJob(nextNode *Node) error {
	if reflect.TypeOf(j.f.OutputObject()) != reflect.TypeOf(nextNode.f.InputObject()) {
		return errors.New(fmt.Sprintf("%s的输出类型和%s的输入类型不一致:%s!=%s", j.name, nextNode.name, reflect.TypeOf(j.f.OutputObject()).String(), reflect.TypeOf(nextNode.f.InputObject()).String()))
	}
	j.nextNode = nextNode
	return nil
}

// SetErrNode 设置处理失败结果的Job
func (j *Node) SetErrNode(errNode *Node) error {
	if reflect.TypeOf(ErrOutput{}) != reflect.TypeOf(errNode.f.InputObject()) {
		return errors.New(fmt.Sprintf("%s的输入类型不是ErrOutput", errNode.name))
	}
	j.errNode = errNode
	if j.nextNode != nil {
		_ = j.nextNode.SetErrNode(errNode)
	}
	return nil
}

// Start 加载并启动
func (j *Node) Start() (err error) {
	if j.status == StartingStatus || j.status == StartedStatus {
		return nil
	}
	defer fmt.Println("started:", j.name)
	if !j.statusMtx.TryLock() {
		return TransitionsErr
	}
	defer j.statusMtx.Unlock()
	j.status = StartingStatus

	// 当有nextJob时，输出会传入到nextJob
	if j.nextNode == nil {
		// 指定输出通道
		j.outputCh = make(chan TaskOutput)
	} else {
		err = j.nextNode.Start()
		if err != nil {
			return err
		}
		defer func() {
			if err != nil {
				e := j.nextNode.Stop()
				if e != nil {
					println("关闭任务失败:", e.Error())
				}
			}
		}()
	}

	j.inputCh, err = queue.NewQueue(j.name, j.f.InputObject(), j.bufferSize)
	if err != nil {
		return err
	}
	// 起一个协程开始处理
	for i := 0; i < j.concurrency; i++ {
		go func() {
			j.runWG.Add(1)
			defer j.runWG.Done()
			j.run()
		}()
	}
	j.status = StartedStatus
	return nil
}

// Stop 停止并存储
func (j *Node) Stop() (err error) {
	if !j.statusMtx.TryLock() {
		return TransitionsErr
	}
	defer j.statusMtx.Unlock()
	j.status = StoppingStatus

	if j.nextNode != nil {
		defer func() {
			if err == nil {
				err = j.nextNode.Stop()
			}
		}()
	}

	// 持久化待处理数据
	err = j.inputCh.Close()
	if err != nil {
		return err
	}
	if j.outputCh != nil {
		defer close(j.outputCh)
	}

	j.runWG.Wait()
	j.status = StoppedStatus
	return nil
}

// Pause 暂停从输入中读取任务
func (j *Node) Pause() error {
	if !j.statusMtx.TryLock() {
		return TransitionsErr
	}
	defer j.statusMtx.Unlock()
	//
	return nil
}

// Resume 从暂停状态恢复到运行状态
func (j *Node) Resume() error {
	if !j.statusMtx.TryLock() {
		return TransitionsErr
	}
	defer j.statusMtx.Unlock()
	//
	return nil
}

// AddTask 添加要执行的任务到队列
func (j *Node) AddTask(task Task) error {
	if j.status == StartedStatus || j.status == StartingStatus {
		return j.inputCh.Push(task.UUID, task.Input)
	}
	return IsStoppedErr
}

func (j *Node) CancelTask(uuid string) bool {
	return j.inputCh.Remove(uuid)
}

func (j *Node) GetResult() <-chan TaskOutput {
	if j.nextNode != nil {
		return j.nextNode.GetResult()
	}
	return j.outputCh
}

// GetStatus 获取Job的状态，主要包含待处理id队列、正在处理的ids
func (j *Node) GetStatus() *StatusInfo {
	var status = StatusInfo{
		Name:        j.name,
		Status:      statusNameMap[j.status],
		CurrentTask: j.current,
		PendingList: j.inputCh.List(),
	}
	if j.nextNode != nil {
		status.NextStatus = j.nextNode.GetStatus()
	}
	return &status
}

type StatusInfo struct {
	// Name Job名称
	Name string `json:"name"`
	// Status Job状态
	Status string `json:"status"`
	// CurrentTask 当前在处理的任务
	CurrentTask string `json:"current_task"`
	// PendingList 等待任务队列
	PendingList []string `json:"pending_list"`
	// NextStatus 下个节点的状态
	NextStatus *StatusInfo `json:"next_status,omitempty"`
}

func (j *Node) run() {
	var task Task
	var err error
	var outputs []any

	for {
		j.current = ""
		task.UUID, task.Input = j.inputCh.Pop()
		j.current = task.UUID
		if task.UUID == "" { // uuid为空表示输入chan已关闭
			return
		}

		outputs, err = j.f.Work(task.UUID, task.Input)
		if err != nil {
			if j.errNode != nil {
				err = j.errNode.AddTask(Task{
					UUID: task.UUID,
					Input: ErrOutput{
						Outputs: outputs,
						Err:     err,
					},
				})
				if err != nil {
					println("输出错误信息失败:", err.Error())
				}
			}
		}
		for _, output := range outputs {
			if j.nextNode != nil {
				err = j.nextNode.AddTask(Task{
					UUID:  task.UUID,
					Input: output,
				})
			} else {
				j.outputCh <- TaskOutput{
					UUID:  task.UUID,
					Input: output,
				}
			}
			if err != nil {
				println("输出错误信息失败:", err.Error())
			}
		}
	}
}
