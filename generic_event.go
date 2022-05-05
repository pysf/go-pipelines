package pipeline

type GenericEvent struct {
	Err  error
	Done *func()
}

func (ee *GenericEvent) GetError() error {
	return ee.Err
}

func (ge *GenericEvent) GetOnDone() *func() {
	return ge.Done
}
