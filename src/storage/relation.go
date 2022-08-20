package storage

/*
type Student struct {
	id   int32
	name string
	age  int32
}

func NewStudent(id int32, name string, age int32) *Student {
	return &Student{
		id,
		name,
		age,
	}
}

func (s *Student) ID() int32 {
	return s.id
}

func (s *Student) Name() string {
	return s.name
}

func (s *Student) Age() int32 {
	return s.age
}

type Table struct {
	rows []*Student
}

func (t *Table) Cardinality() int {
	return len(t.rows)
}

func (t *Table) GetRowsGroup(idx int) ([]*vector.Vector, error) {

	if t.Cardinality()%1024 != 0 {
		return nil, errors.New("illegal rows")
	}

	if 1024*idx >= t.Cardinality() {
		return nil, nil
	}

	ids := make([]int32, 1024)
	names := make([]string, 1024)
	ages := make([]int32, 1024)

	// Deep Copy Data
	base := idx * 1024
	for i := idx * 1024; i < (idx+1)*1024; i++ {
		ids[i-base] = t.rows[i].ID()
		names[i-base] = t.rows[i].Name()
		ages[i-base] = t.rows[i].Age()
	}

	id_vec := vector.NewVectorInt32(ids)
	name_vec := vector.NewVectorString(names)
	age_vec := vector.NewVectorInt32(ages)
	res := []*vector.Vector{id_vec, name_vec, age_vec}
	return res, nil
}
*/
