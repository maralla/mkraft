package plugs

var _ StateMachineIface = (*StateMachineNoOpImpl)(nil)

type StateMachineIface interface {

	// maki: this part is very important, need to discuss with professor and refer to other implementations
	// shall be implemented asynchronosly so that one slow command will not block the whole cluster for client command processing
	ApplyCommand(command []byte) ([]byte, error)

	// state machine should be able to ensure the commandList order is well maintained
	// if the command cannot be applied, the error should be encoded in the []byte as binary payload following the statemachine's protocol
	// if the whole command cannot be applied, use the error
	BatchApplyCommand(commandList [][]byte, index uint64) ([][]byte, error)

	GetLatestAppliedIndex() uint64
}

type StateMachineNoOpImpl struct {
}

func NewStateMachineNoOpImpl() *StateMachineNoOpImpl {
	return &StateMachineNoOpImpl{}
}

func (s *StateMachineNoOpImpl) ApplyCommand(command []byte) ([]byte, error) {
	// the command shall contain the command index
	return []byte("no op"), nil
}

func (s *StateMachineNoOpImpl) GetLatestAppliedIndex() uint64 {
	return 0
}

func (s *StateMachineNoOpImpl) BatchApplyCommand(commandList [][]byte, index uint64) ([][]byte, error) {
	return nil, nil
}
