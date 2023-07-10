package queue

import (
	"testing"
	"time"
)

func TestNewQueue(t *testing.T) {
	q, err := NewQueue("用户", TT{}, 10)
	if err != nil {
		t.Error(err)
		return
	}
	defer func() {
		err := q.Close()
		if err != nil {
			t.Error(err)
		}
	}()

	err = q.Push("5", TT{
		Name: "111",
		Sex:  1,
	})
	if err != nil {
		t.Error(err)
		return
	}

	go func() {
		uuid, item := q.Pop()
		t.Log(uuid, item.(TT))
	}()
	time.Sleep(time.Second)
}

type TT struct {
	Name string `json:"name"`
	Sex  int    `json:"sex"`
}
