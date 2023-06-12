package jobflow

import (
	"fmt"
	"testing"
	"time"
)

type TestJobFunc struct {
	*BaseFunc
}

func NewTestJobFunc() *TestJobFunc {
	return &TestJobFunc{NewBaseFunc()}
}

func (j *TestJobFunc) Do(input Input) {
	var output any
	println("do start")
	time.Sleep(time.Second * 3)
	switch i := input.Input.(type) {
	case int:
		output = fmt.Sprintf("%d 是int类型", i)
	case string:
		output = fmt.Sprintf("%s 是string类型", i)
	}
	println("do end")
	j.SetResult(Output{
		ID:     input.ID,
		Output: output,
		Error:  nil,
	})
}

func TestJobFuncDo(t *testing.T) {
	j := RunJob("test", NewTestJobFunc(), 0)
	go func() {
		// 输入N个要处理的内容
		j.Input(Input{
			ID:    "1",
			Input: 3,
		})

		j.Input(Input{
			ID:    "2",
			Input: "hahaha",
		})

		j.Stop()
	}()

	// 在另一个协程中处理返回的结果
	for output := range j.OutputCh() {
		if output.Error != nil {
			t.Error("处理失败:", output.Error)
		} else {
			t.Log("处理结果:", output.Output)
		}
	}
}
