package featurerollout

import "hash/fnv"

// bucket 返回用户在指定功能下的确定性分桶（0-99）。
//
// 分桶只依赖功能名与用户标识，与规则版本无关：同一用户面对同一规则版本
// 结果始终稳定，且百分比上调只会在原有受众基础上扩大、下调只会收缩，
// 不会因为版本切换而重新洗牌。
func bucket(feature, userID string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(feature))
	_, _ = h.Write([]byte{0x1f})
	_, _ = h.Write([]byte(userID))
	return int(h.Sum32() % 100)
}
