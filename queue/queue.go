package queue

import (
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"sync"

	_ "github.com/mattn/go-sqlite3"

	"go-jobflow/json"
)

// Queue 输入通道
type Queue struct {
	db   *sql.DB
	name string
	typ  reflect.Type
	size int
	ch   chan struct{}
	l    []record
	mtx  sync.Mutex
}

type record struct {
	uuid    string
	item    any
	deleted bool
}

var (
	NameErr = errors.New("name重复注册")
	ItemErr = errors.New("item类型和NewQueue时定义的类型不一致")
)
var nameMap = make(map[string]bool)

func NewQueue(name string, item any, size int) (*Queue, error) {
	if nameMap[name] {
		return nil, NameErr
	}
	nameMap[name] = true
	db, err := sql.Open("sqlite3", "queue.db")
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// make sql
	createSql := `CREATE TABLE IF NOT EXISTS "` + name + `" (id integer PRIMARY KEY AUTOINCREMENT, uuid TEXT NOT NULL, data TEXT NOT NULL)`
	_, err = db.Exec(createSql)
	if err != nil {
		return nil, err
	}

	querySql := `select * from ` + name
	rows, err := db.Query(querySql)
	if err != nil {
		return nil, err
	}

	c := &Queue{
		db:   db,
		name: name,
		typ:  reflect.TypeOf(item),
		size: size,
		ch:   make(chan struct{}, size),
		l:    make([]record, 0, size),
	}

	for rows.Next() {
		var _id int // sqlite的自增ID，只是用来支持存储重复数据
		var uuid string
		var data []byte
		err = rows.Scan(&_id, &uuid, &data)
		if err != nil {
			return nil, err
		}

		// 获取到Input类型的指针类型, 这种类型在json.Unmarshal时会保留
		aa := reflect.New(c.typ).Interface()
		err = json.Unmarshal(data, aa)
		if err != nil {
			return nil, err
		}
		bb := reflect.ValueOf(aa).Elem().Interface()
		err = c.Push(uuid, bb)
		if err != nil {
			return nil, err
		}
	}
	// 加载到内存后清理db
	_, err = db.Exec(`delete from ` + name)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Queue) Close() error {
	db, err := sql.Open("sqlite3", "queue.db")
	if err != nil {
		return err
	}
	defer db.Close()

	insertSql := `insert into "` + c.name + `" (uuid, data) values(?,?)`
	insertStmt, err := db.Prepare(insertSql)
	if err != nil {
		return err
	}

	c.mtx.Lock()
	defer c.mtx.Unlock()

	//// 提前扔出去一个，避免当close时，c.ch写阻塞，会导致写入协程panic
	close(c.ch)

	// 清空channel
	count := 0
	for _ = range c.ch {
		count++
	}
	// 保存list所有数据
	for _, r := range c.l[:count] {
		// 逐个存储起来
		tmp, err := json.Marshal(r.item)
		if err != nil {
			return err
		}
		_, err = insertStmt.Exec(r.uuid, tmp)
		if err != nil {
			return err
		}
	}
	return nil
}

// Push 阻塞推入
func (c *Queue) Push(uuid string, item any) (err error) {
	if uuid == "" {
		return errors.New("uuid不可为空")
	}
	if reflect.TypeOf(item) != c.typ {
		return ItemErr
	}

	c.mtx.Lock()
	defer func() {
		defer func() {
			// 关闭后再推入则报错
			if e := recover(); e != nil {
				err = e.(error)
			}
		}()
		c.ch <- struct{}{}
	}()
	defer c.mtx.Unlock()
	c.l = append(c.l, record{uuid, item, false})
	return nil
}

// Pop 阻塞式弹出
func (c *Queue) Pop() (string, any) {
	defer func() {
		if e := recover(); e != nil {
			fmt.Println("panic:", e.(error))
		}
	}()
	for {
		_, ok := <-c.ch
		if !ok {
			return "", nil
		}

		c.mtx.Lock()
		// 弹出内容
		r := c.l[0]
		c.l = c.l[1:]
		c.mtx.Unlock()
		if r.deleted { // 若该条已经被删除，则取下一条
			continue
		}
		return r.uuid, r.item
	}
}

func (c *Queue) Remove(uuid string) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for i, r := range c.l {
		if r.deleted {
			continue
		}
		if r.uuid == uuid {
			c.l[i].deleted = true
			return true
		}
	}
	return false
}

func (c *Queue) List() []string {
	var l = make([]string, 0, len(c.l))
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for _, r := range c.l {
		if r.deleted {
			continue
		}
		l = append(l, r.uuid)
	}
	return l
}
