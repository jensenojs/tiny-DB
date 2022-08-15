package operator

import "tiny-db/src/execution/executor"

type limit struct {
	operator executor.Operator
	operatorState executor.OperatorState
}