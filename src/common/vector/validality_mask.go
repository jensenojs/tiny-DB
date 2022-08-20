package vector

type validality_t uint64

const (
	bitPerValue int    = 64
	maxEntry    uint64 = ^uint64(0) // 0xFFFFFFFFFFFFFFFF
)

// TODO(lokax): Add more function.
// Combine,

//! ValidalityMask is used to represent a value is NULL(NOT NULL).
//! 1 --> Valid(NOT NULL), 0 --> InValid(NULL)
type ValidalityMask struct {
	validalityMask []validality_t
}

//! Create a new empty mask. it means all values are valid.
func NewValidalityMask() *ValidalityMask {
	return &ValidalityMask{}
}

func EntryCount(count int) int {
	return (count + bitPerValue - 1) / bitPerValue
}

func (v *ValidalityMask) AllValid() bool {
	return v.validalityMask == nil
}

func GetEntryIndex(idx int) (int, int) {
	entryIdx := idx / bitPerValue
	inIdx := idx % bitPerValue
	return entryIdx, inIdx
}

func RowIsValid(mask validality_t, inIdx int) bool {
	return (mask & (validality_t(1) << validality_t(inIdx))) != 0
}

func RowIsInValid(mask validality_t, inIdx int) bool {
	return (mask & (validality_t(1) << validality_t(inIdx))) == 0
}

func (v *ValidalityMask) GetValidalityEntry(entry_idx int) validality_t {
	if v.validalityMask == nil {
		return validality_t(maxEntry)
	}
	return v.validalityMask[entry_idx]
}

func (v *ValidalityMask) RowIsValidLike(idx int) bool {
	entryIdx, inIdx := GetEntryIndex(idx)
	mask := v.GetValidalityEntry(entryIdx)
	return RowIsValid(mask, inIdx)
}

func (v *ValidalityMask) RowIsValid(idx int) bool {
	// fast path, nil means all values are valid
	if v.validalityMask == nil {
		return true
	}
	return v.RowIsValidLike(idx)
}

func (v *ValidalityMask) RowIsInValidLike(idx int) bool {
	entryIdx, inIdx := GetEntryIndex(idx)
	mask := v.GetValidalityEntry(entryIdx)
	return RowIsInValid(mask, inIdx)
}

func (v *ValidalityMask) RowIsInValid(idx int) bool {
	// fast path
	if v.validalityMask == nil {
		return false
	}
	return v.RowIsInValidLike(idx)
}

func (v *ValidalityMask) SetRowValid(idx int) {
	// fast path
	if v.validalityMask == nil {
		return
	}
	entryIdx, inIdx := GetEntryIndex(idx)
	v.validalityMask[entryIdx] |= (validality_t(1) << validality_t(inIdx))
}

func (v *ValidalityMask) SetRowInValid(idx int) {
	if v.validalityMask == nil {
		v.initialize(COMMON_VECTOR_SIZE)
	}
	entryIdx, inIdx := GetEntryIndex(idx)
	v.validalityMask[entryIdx] &= ^(validality_t(1) << validality_t(inIdx))
}

func (v *ValidalityMask) GetData() []validality_t {
	return v.validalityMask
}

func (v *ValidalityMask) initialize(count int) {
	v.validalityMask = make([]validality_t, EntryCount(count))
}
