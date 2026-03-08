package kb

import (
	"reflect"
)

// Validator 验证
type Validator interface {
	// Execute 执行检查
	Execute(data any) bool
}

// NonEmptyValidator 非空验证
type NonEmptyValidator struct {
}

// Execute 执行检查
func (v *NonEmptyValidator) Execute(data any) bool {
	if data == nil {
		return false
	}

	content := reflect.ValueOf(data)
	for content.Kind() == reflect.Ptr {
		if content.IsNil() {
			return false
		}
		content = content.Elem()
	}

	switch content.Kind() {
	case reflect.String, reflect.Array, reflect.Slice, reflect.Map, reflect.Chan:
		return content.Len() > 0
	default:
		return false
	}
}

// RangeValidator 范围验证
type RangeValidator struct {
	Min, Max *float64
}

// Execute 执行检查
func (v *RangeValidator) Execute(data any) bool {
	if data == nil {
		return false
	}

	value := float64(0)
	content := reflect.ValueOf(data)
	for content.Kind() == reflect.Ptr {
		if content.IsNil() {
			return false
		}
		content = content.Elem()
	}

	switch content.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		value = float64(content.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		value = float64(content.Uint())
	case reflect.Float32, reflect.Float64:
		value = content.Float()
	default:
		return false
	}

	return (v.Min == nil || value >= *v.Min) && (v.Max == nil || value <= *v.Max)
}

// RangeOptionFunction 范围选项
type RangeOptionFunction func(*RangeValidator)

// WithMin 指定最小值
func WithMin(min float64) RangeOptionFunction {
	return func(o *RangeValidator) {
		o.Min = new(float64)
		*o.Min = min
	}
}

// WithMax 指定最小值
func WithMax(max float64) RangeOptionFunction {
	return func(o *RangeValidator) {
		o.Max = new(float64)
		*o.Max = max
	}
}

// NewRangeValidator 创建范围验证
func NewRangeValidator(options ...RangeOptionFunction) Validator {
	validator := &RangeValidator{}
	for _, option := range options {
		option(validator)
	}

	return validator
}

// DataValidation 数据验证
type DataValidation struct {
	Data      any
	Validator Validator
}

// VerifyData 验证数据
func VerifyData(validationList []*DataValidation) bool {
	for _, item := range validationList {
		if item.Validator != nil &&
			!item.Validator.Execute(item.Data) {
			return false
		}
	}

	return true
}
