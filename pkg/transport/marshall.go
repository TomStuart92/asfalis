package transport

import "fmt"

// Marshaler presents an interface for marshalling data
type Marshaler interface {
	Marshal() (data []byte, err error)
}

// Unmarshaler presents an interface for marshalling data
type Unmarshaler interface {
	Unmarshal(data []byte) error
}

// MustMarshal is a wrapper which catches any errors from an attempted Marshal
func MustMarshal(m Marshaler) []byte {
	d, err := m.Marshal()
	if err != nil {
		fmt.Printf("marshal should never fail (%v)", err)
	}
	return d
}

// MustUnmarshal is a wrapper which catches any errors from an attempted Unmarshal
func MustUnmarshal(um Unmarshaler, data []byte) {
	if err := um.Unmarshal(data); err != nil {
		fmt.Printf("unmarshal should never fail (%v)", err)
	}
}
