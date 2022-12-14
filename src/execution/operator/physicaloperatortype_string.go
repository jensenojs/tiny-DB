// Code generated by "stringer -type=PhysicalOperatorType -trimprefix=Physical"; DO NOT EDIT.

package operator

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[PhysicalInvalid-0]
	_ = x[PhysicalTableScan-1]
	_ = x[PhysicalFilter-2]
	_ = x[PhysicalHashJoin-3]
	_ = x[PhysicalLimit-4]
	_ = x[PhysicalCollector-5]
}

const _PhysicalOperatorType_name = "InvalidTableScanFilterHashJoinLimitCollector"

var _PhysicalOperatorType_index = [...]uint8{0, 7, 16, 22, 30, 35, 44}

func (i PhysicalOperatorType) String() string {
	if i < 0 || i >= PhysicalOperatorType(len(_PhysicalOperatorType_index)-1) {
		return "PhysicalOperatorType(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _PhysicalOperatorType_name[_PhysicalOperatorType_index[i]:_PhysicalOperatorType_index[i+1]]
}
