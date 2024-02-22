package pyt

import (
	"fmt"
	"strings"
)

type FilterSet []*filter

// Build does the work of converting all of the filter instances
// into a where clause. It will also append any values to the params slice
// that is used in the final query
func (fs FilterSet) Build(params *[]any) string {
	if len(fs) == 0 {
		return ""
	}

	res := strings.Builder{}
	res.WriteString("(")

	max := len(fs) - 1
	for i, f := range fs {
		if f == nil {
			continue
		}

		res.WriteString(f.Build(params))

		if i != max {
			res.WriteString(fmt.Sprintf(` %v `, f.SubFilterComparision))
		}
	}

	res.WriteString(")")

	return res.String()
}

type filter struct {
	Field                string
	Comparision          string
	Value                any
	SubFilterComparision string
	SubFilter            FilterSet
}

// Build does the work of converting the filter instance and any sub-fitlers
// into a where clause. It will also append any values to the params slice
// that is used in the final query
func (f filter) Build(params *[]any) string {
	*params = append(*params, f.Value)
	sub := f.SubFilter.Build(params)
	if strings.TrimSpace(sub) != "" {
		sub = fmt.Sprintf(`%s %s`, f.SubFilterComparision, sub)
	}

	fil := fmt.Sprintf(`%s%s? %s`, f.Field, f.Comparision, sub)

	return fil
}

// Add will register a subfilter on this filer
func (f *filter) Add(sub *filter) {
	f.SubFilter = append(f.SubFilter, sub)
}

// NewFilter will create a filter with a default equal (=) Comparision
// and a default "and" SubFilterComparision
func NewFilter(field string, value any, subFilters ...*filter) *filter {
	return NewFilterFull(field, "=", value, "and", subFilters...)
}

// NewOrFilter will create a filter with a default equal (=) Comparision
// and a default "or" SubFilterComparision
func NewOrFilter(field string, value any, subFilters ...*filter) *filter {
	return NewFilterFull(field, "=", value, "or", subFilters...)
}

// NewFilterFull builds a filter
func NewFilterFull(field, comparison string, value any, subFilterComparision string, subFilters ...*filter) *filter {
	return &filter{
		Field:                field,
		Comparision:          comparison,
		Value:                value,
		SubFilterComparision: subFilterComparision,
		SubFilter:            subFilters,
	}
}
