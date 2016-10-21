package ssdb

type OrderedMap interface {
	Keys() []string
	Values() []string
	Length() int
	Index(int) (key string, value string)
	Lookup(key string) (value string, exists bool)

	// For iteration.
	Next() (key string, value string, end bool)
	Reset()
}

func NewMap(s []string) OrderedMap {
	om := &orderedMap{}
	for i := 0; i < len(s); i = i + 2 {
		om.keys = append(om.keys, s[i])
		om.values = append(om.values, s[i+1])
	}

	return om
}

type orderedMap struct {
	keys   []string
	values []string
	iter   int
}

func (om *orderedMap) Keys() []string {
	return om.keys
}

func (om *orderedMap) Values() []string {
	return om.values
}

func (om *orderedMap) Length() int {
	return len(om.keys)
}

func (om *orderedMap) Index(i int) (key string, value string) {
	return om.keys[i], om.values[i]
}

func (om *orderedMap) Lookup(key string) (value string, exists bool) {
	// TODO : it has a bad lookup algo, using a map instead?
	for i, k := range om.keys {
		if key == k {
			return om.values[i], true
		}
	}
	return "", false
}

func (om *orderedMap) Next() (key string, value string, end bool) {
	if om.iter >= len(om.keys) {
		return "", "", true
	}
	it := om.iter
	om.iter++
	return om.keys[it], om.values[it], false
}

func (om *orderedMap) Reset() {
	om.iter = 0
}
