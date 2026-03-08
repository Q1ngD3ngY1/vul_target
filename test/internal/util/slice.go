package util

func TranslateTo[T any](source []T, target []T, key func(T) string) (delete []T, exist [][2]T, add []T, err error) {
	exist = make([][2]T, 0, 2)
	add = make([]T, 0, 2)
	delete = make([]T, 0, 2)
	// 使用map减少循环次数
	sourceMap := make(map[string]T, len(source))
	targetMap := make(map[string]T, len(target))

	for _, v := range source {
		sourceMap[key(v)] = v
	}
	for _, v := range target {
		targetMap[key(v)] = v
	}

	for _, s := range source {
		found := false
		if v, ok := targetMap[key(s)]; ok {
			found = true
			exist = append(exist, [2]T{s, v})
		}
		if !found {
			delete = append(delete, s)
		}
	}
	for _, t := range target {
		found := false
		if _, ok := sourceMap[key(t)]; ok {
			found = true
		}
		if !found {
			add = append(add, t)
		}
	}
	return delete, exist, add, nil
}
