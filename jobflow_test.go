package jobflow

import (
	"strconv"
	"testing"
	"time"
)

var startTime = time.Now()

type testFunc1 struct {
	*BaseFunc
}

func (f *testFunc1) Do(input Input) {
	time.Sleep(2 * time.Second)
	f.SetResult(Output{
		ID:     input.ID,
		Output: input.Input,
	})
}

type testFunc2 struct {
	*BaseFunc
}

func (f *testFunc2) Do(input Input) {
	time.Sleep(3 * time.Second)
	f.SetResult(Output{
		ID:     input.ID,
		Output: input.Input,
	})
}

func TestJobFlowFunc(t *testing.T) {
	jobFlow, err := NewJobFlowFunc([]*Job{RunJob("testFunc1", &testFunc1{NewBaseFunc()}, 0),
		RunJob("testFunc2", &testFunc2{NewBaseFunc()}, 0)})
	if err != nil {
		t.Error(err)
		return
	}

	j := RunJob("jobFlow", jobFlow, 2)
	go func() {
		for i := 1; i <= 5; i++ {
			j.Input(Input{ID: strconv.Itoa(i), Input: i})
			t.Log(time.Since(startTime), "输入:", i)
		}
		time.Sleep(time.Second)
		j.Stop()
	}()

	time.Sleep(time.Second)
	for output := range j.OutputCh() {
		if output.Error != nil {
			t.Error(time.Since(startTime), "处理失败:", output.Error)
		} else {
			t.Log(time.Since(startTime), "处理结果:", output.Output)
		}
	}
}
