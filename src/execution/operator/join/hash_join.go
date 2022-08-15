package operator

import "tiny-db/src/execution/executor"

type join struct {
	operator executor.Operator
	operatorState executor.OperatorState
}