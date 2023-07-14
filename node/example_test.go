package node

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"go-jobflow/json"
)

type Exam1 struct {
}

func (p *Exam1) InputObject() any {
	return int(0)
}

func (p *Exam1) OutputObject() any {
	return ""
}

func (p *Exam1) Work(uuid string, input any) (outputs []any, err error) {
	count := input.(int)
	data := strconv.Itoa(count)
	//fmt.Println("Exam1正在处理:", input)
	for i := 0; i < count; i++ {
		outputs = append(outputs, data)
	}
	time.Sleep(time.Second)
	return
}

type Exam2 struct {
}

func (p *Exam2) InputObject() any {
	return ""
}

func (p *Exam2) OutputObject() any {
	return ""
}

func (p *Exam2) Work(uuid string, input any) (output []any, err error) {
	data := input.(string)
	data = "[" + data + "]"
	//fmt.Println("Exam2正在处理:", input)
	time.Sleep(time.Second * 1)
	return []any{data}, nil
}

func TestExam(t *testing.T) {
	f := NewNode("Exam1", 4, 1, &Exam1{})
	err := f.AppendJobs(NewNode("Exam2", 3, 3, &Exam2{}))
	if err != nil {
		t.Error(err)
		return
	}

	err = f.Start()
	if err != nil {
		t.Error("启动失败:", err)
		return
	}
	wg := sync.WaitGroup{}
	defer wg.Wait() // 使用wg保证所有输出都现实完成后才退出主协程
	defer f.Stop()

	go func() {
		wg.Add(1)
		defer wg.Done()
		for output := range f.GetResult() {
			t.Logf("id:%s, 输出:%v", output.UUID, output.Input)
		}
	}()

	go func() {
		for i := 1; i <= 6; i++ {
			uuid := strconv.FormatInt(time.Now().UnixMilli(), 10)
			task := Task{
				UUID:  uuid,
				Input: i,
			}
			err := f.AddTask(task)
			if err != nil {
				println("输入错误:", err.Error())
			} else {
				t.Log("输入:", task.Input)
			}
			time.Sleep(time.Second)
		}
	}()

	go func() {
		for {
			time.Sleep(time.Second)
			status := f.GetStatus()
			tmp, _ := json.Marshal(status)
			t.Log(string(tmp))
		}
	}()

	time.Sleep(time.Second * 10)
}
