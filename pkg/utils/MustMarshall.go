package utils

import "fmt"

type Marshaler interface {
	Marshal() (data []byte, err error)
}

type Unmarshaler interface {
	Unmarshal(data []byte) error
}

func MustMarshal(m Marshaler) []byte {
	d, err := m.Marshal()
	if err != nil {
		fmt.Printf("marshal should never fail (%v)", err)
	}
	return d
}

func MustUnmarshal(um Unmarshaler, data []byte) {
	if err := um.Unmarshal(data); err != nil {
		fmt.Printf("unmarshal should never fail (%v)", err)
	}
}

func MaybeUnmarshal(um Unmarshaler, data []byte) bool {
	if err := um.Unmarshal(data); err != nil {
		return false
	}
	return true
}

func GetBool(v *bool) (vv bool, set bool) {
	if v == nil {
		return false, false
	}
	return *v, true
}

func Boolp(b bool) *bool { return &b }
