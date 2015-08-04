package parquet

import "fmt"

// PLAIN encoding for BOOLEAN type: bit-packed, LSB first

type booleanPlainEncoder struct {
	data      []byte
	value     bool
	err       error
	numValues int32

	n      int32
	values []bool
}

func (e *booleanPlainEncoder) init(data []byte, numValues int32) {
	e.data = data
	e.numValues = numValues

	e.n = 0
	e.values = nil
}

func (e *booleanPlainEncoder) next() bool {
	// TODO: implement properly
	if e.values == nil {
		e.values = make([]bool, e.numValues)
		j := -1
		k := 7
		var b byte
		for i := int32(0); i < e.numValues; i++ {
			if k == 7 {
				k = 0
				j++
				if j >= len(e.data) {
					e.err = fmt.Errorf("overflow")
					return false
				}
				b = e.data[j]
			}
			e.values[i] = (b&1 == 1)
			b >>= 1
		}
	}
	if e.n >= e.numValues {
		return false
	}
	e.value = e.values[e.n]
	e.n++
	return true
}
