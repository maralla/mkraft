package pluggable

var _ StateMachineIface = (*StateMachineNoOpImpl)(nil)

type StateMachineIface interface {
	ApplyCommand(command []byte, index uint64) ([]byte, error)
	GetLatestAppliedIndex() uint64
}

type StateMachineNoOpImpl struct {
}

func NewStateMachineNoOpImpl() *StateMachineNoOpImpl {
	return &StateMachineNoOpImpl{}
}

func (s *StateMachineNoOpImpl) ApplyCommand(command []byte, index uint64) ([]byte, error) {
	return []byte("no op"), nil
}

func (s *StateMachineNoOpImpl) GetLatestAppliedIndex() uint64 {
	return 0
}
