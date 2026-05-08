package retryqueue

// 默认 noop，可以通过 WithMetrics 注入 prometheus
// TODO 待实现   prometheus
type Metrics interface {
	Enqueue(ok bool)
	Ack()
	Nack(attempt int)
	Dispatch(count int)              //记录任务投递，count 是本次投递数量
	DispatchLatency(seconds float64) // 记录投递延迟（秒
}

type noopMetrics struct{}

func (noopMetrics) Enqueue(ok bool)                 {}
func (noopMetrics) Ack()                            {}
func (noopMetrics) Nack(attempt int)                {}
func (noopMetrics) Dispatch(count int)              {}
func (noopMetrics) DispatchLatency(seconds float64) {}
