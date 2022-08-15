package operator

import "tiny-db/src/execution/executor"

type table_scan struct {
	operator executor.Operator
	operatorState executor.OperatorState
}