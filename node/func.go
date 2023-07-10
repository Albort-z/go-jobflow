package node

type Func interface {
	InputObject() any                                      // 返回输入参数对象
	OutputObject() any                                     // 返回输出参数对象
	Work(uuid string, input any) (output []any, err error) // 处理任务的方法, 其input和output就是输入和输出参数, 输入参数需要可持久化
}
