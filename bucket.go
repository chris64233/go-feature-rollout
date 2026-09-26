package featurerollout

import (
	"crypto/sha256"
	"encoding/binary"
)

// bucketGranularity 分桶粒度：把哈希空间切成 10000 个桶，百分比精确到 0.01%。
const bucketGranularity = 10000

// inBucket 判定用户是否落在某功能的稳定百分桶内。
//
// 以 (功能, 用户) 为输入做确定性哈希，取模后与百分比阈值比较：
//   - 同一用户面对同一规则版本，结果永远稳定；
//   - 桶号与版本无关，因此百分比调大时新受众严格包含旧受众（嵌套桶），
//     调小时严格收缩——百分比变化只会确定性地扩大或收缩受众，
//     已在桶内的用户不会因放量而掉出。
func inBucket(feature FeatureKey, user UserID, percentage int) bool {
	h := sha256.New()
	h.Write([]byte(feature))
	h.Write([]byte{0})
	h.Write([]byte(user))
	sum := h.Sum(nil)
	bucket := int(binary.BigEndian.Uint64(sum[:8]) % uint64(bucketGranularity))
	return bucket < percentage*(bucketGranularity/100)
}
