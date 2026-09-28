// Package featurerollout 实现功能灰度发布服务：
// 多功能版本整组原子发布、确定性分桶判定、前置依赖、幂等发布、快照回滚，
// 以及基于线上结果归因统计的自动暂停守卫（exposure guard）。
package featurerollout
