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

func EntryCount(count uint64) uint64 {
	return (count + uint64(bitPerValue-1)) / uint64(bitPerValue)
}

func (v *ValidalityMask) AllValid() bool {
	return v.validalityMask == nil
}

func GetEntryIndex(idx uint64) (uint64, uint64) {
	entryIdx := idx / uint64(bitPerValue)
	inIdx := idx % uint64(bitPerValue)
	return entryIdx, inIdx
}

func RowIsValid(mask validality_t, inIdx uint64) bool {
	return (mask & (validality_t(1) << validality_t(inIdx))) != 0
}

func RowIsInValid(mask validality_t, inIdx uint64) bool {
	return (mask & (validality_t(1) << validality_t(inIdx))) == 0
}

func (v *ValidalityMask) GetValidalityEntry(entry_idx uint64) validality_t {
	if v.validalityMask == nil {
		return validality_t(maxEntry)
	}
	return v.validalityMask[entry_idx]
}

func (v *ValidalityMask) RowIsValidLike(idx uint64) bool {
	entryIdx, inIdx := GetEntryIndex(idx)
	mask := v.GetValidalityEntry(entryIdx)
	return RowIsValid(mask, inIdx)
}

func (v *ValidalityMask) RowIsValid(idx uint64) bool {
	// fast path, nil means all values are valid
	if v.validalityMask == nil {
		return true
	}
	return v.RowIsValidLike(idx)
}

func (v *ValidalityMask) RowIsInValidLike(idx uint64) bool {
	entryIdx, inIdx := GetEntryIndex(idx)
	mask := v.GetValidalityEntry(entryIdx)
	return RowIsInValid(mask, inIdx)
}

func (v *ValidalityMask) RowIsInValid(idx uint64) bool {
	// fast path
	if v.validalityMask == nil {
		return false
	}
	return v.RowIsInValidLike(idx)
}

func (v *ValidalityMask) SetRowValid(idx uint64) {
	// fast path
	if v.validalityMask == nil {
		return
	}
	entryIdx, inIdx := GetEntryIndex(idx)
	v.validalityMask[entryIdx] |= (validality_t(1) << validality_t(inIdx))
}

func (v *ValidalityMask) SetRowInValid(idx uint64) {
	if v.validalityMask == nil {
		v.initialize(COMMON_VECTOR_SIZE)
	}
	entryIdx, inIdx := GetEntryIndex(idx)
	v.validalityMask[entryIdx] &= ^(validality_t(1) << validality_t(inIdx))
}

func (v *ValidalityMask) GetData() []validality_t {
	return v.validalityMask
}

func (v *ValidalityMask) initialize(count uint64) {
	v.validalityMask = make([]validality_t, EntryCount(count))
}
