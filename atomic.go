package featurerollout

import "sync/atomic"

// atomicSnapshot 当前生效快照的原子指针：写路径整体替换，读路径无锁加载，
// 保证在线判定永远看到一份完整快照而非中间态。
type atomicSnapshot = atomic.Pointer[snapshot]
