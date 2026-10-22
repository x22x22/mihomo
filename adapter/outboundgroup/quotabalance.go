package outboundgroup

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	C "github.com/metacubex/mihomo/constant"
	"github.com/metacubex/mihomo/log"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
)

// QuotaRule 单个出口的配额：最大活跃连接数与最大带宽（Mbps）。
type QuotaRule struct {
	MaxConnections int64 `group:"connections,omitempty"`
	MaxBandwidth   int64 `group:"bandwidth,omitempty"`
}

type QuotaBalanceOption struct {
	// proxy name -> 配额
	Quota map[string]QuotaRule `group:"quota,omitempty"`
}

type quotaStat struct {
	connections atomic.Int64
	bytes       atomic.Int64
}

// QuotaBalance 按配额 + 健康状态的负载均衡组。
//
// 设计要点：连接数/带宽统计完全在独立 goroutine 中旁路采样（每秒一次遍历
// statistic.DefaultManager），写入 atomic 变量；Unwrap 选择路径只读取
// atomic 值，不做任何锁等待或统计遍历，因此统计开销不会阻塞代理核心路径。
type QuotaBalance struct {
	*GroupBase
	testUrl   string
	quotas    map[string]QuotaRule
	stats     sync.Map // name -> *quotaStat
	stopOnce  sync.Once
	stop      chan struct{}
	startOnce sync.Once
	logLast   atomic.Int64 // 上次打印选择日志的时间（UnixNano），限流用
}

// Now 返回当前会选择的节点（与 Unwrap 相同的选择逻辑，用于面板展示）。
func (q *QuotaBalance) Now() string {
	proxies := q.GetProxies(false)
	if len(proxies) == 0 {
		return ""
	}
	healthy := make([]C.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if proxy.AliveForTestUrl(q.testUrl) {
			healthy = append(healthy, proxy)
		}
	}
	if len(healthy) == 0 {
		return proxies[0].Name()
	}
	if len(healthy) == 1 {
		return healthy[0].Name()
	}
	best := healthy[0]
	bestScore := math.MaxFloat64
	for _, proxy := range healthy {
		score := q.saturation(proxy.Name())
		if score < bestScore {
			best = proxy
			bestScore = score
		}
	}
	return best.Name()
}

func (q *QuotaBalance) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxies := q.GetProxies(touch)
	if len(proxies) == 0 {
		if fallback := q.EmptyFallback(); fallback != nil {
			return fallback
		}
		return nil
	}

	healthy := make([]C.Proxy, 0, len(proxies))
	for _, proxy := range proxies {
		if proxy.AliveForTestUrl(q.testUrl) {
			healthy = append(healthy, proxy)
		}
	}
	if len(healthy) == 0 {
		return proxies[0]
	}
	if len(healthy) == 1 {
		return healthy[0]
	}

	best := healthy[0]
	bestScore := math.MaxFloat64
	for _, proxy := range healthy {
		score := q.saturation(proxy.Name())
		if score < bestScore {
			best = proxy
			bestScore = score
		}
	}
	// 稳定性日志（每秒最多一条，避免刷屏）
	now := time.Now().UnixNano()
	last := q.logLast.Load()
	if now-last > int64(time.Second) {
		if q.logLast.CompareAndSwap(last, now) {
			conns, bytes := int64(0), int64(0)
			if v, ok := q.stats.Load(best.Name()); ok {
				st := v.(*quotaStat)
				conns = st.connections.Load()
				bytes = st.bytes.Load()
			}
			log.Infoln("[QuotaBalance] %s -> %s (score=%.2f conns=%d bytes=%d delay=%dms)", q.Name(), best.Name(), bestScore, conns, bytes, best.LastDelayForTestUrl(q.testUrl))
		}
	}
	return best
}

// saturation 返回节点当前的饱和分数：0 表示空闲，>=1 表示达到配额上限。
// 只读 atomic，不阻塞。
func (q *QuotaBalance) saturation(name string) float64 {
	v, ok := q.stats.Load(name)
	if !ok {
		return 0
	}
	st := v.(*quotaStat)
	rule, ok := q.quotas[name]
	if !ok {
		return 0
	}
	var score float64
	if rule.MaxConnections > 0 {
		score += float64(st.connections.Load()) / float64(rule.MaxConnections)
	}
	if rule.MaxBandwidth > 0 {
		mbps := float64(st.bytes.Load()) * 8 / 1e6
		score += mbps / float64(rule.MaxBandwidth)
	}
	return score
}

// startStats 启动旁路统计 goroutine（只采样 statistic.Manager，不参与代理路径）。
func (q *QuotaBalance) startStats() {
	q.startOnce.Do(func() {
		q.stop = make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			prev := make(map[string]int64)
			for {
				select {
				case <-ticker.C:
					counts := make(map[string]int64)
					cur := make(map[string]int64)
					statistic.DefaultManager.Range(func(t statistic.Tracker) bool {
						info := t.Info()
						if info == nil || len(info.Chain) == 0 {
							return true
						}
						name := info.Chain[0]
						counts[name]++
						cur[name] += info.UploadTotal.Load() + info.DownloadTotal.Load()
						return true
					})
					for name, count := range counts {
						v, _ := q.stats.LoadOrStore(name, &quotaStat{})
						st := v.(*quotaStat)
						st.connections.Store(count)
						if p, ok := prev[name]; ok && cur[name] >= p {
							st.bytes.Store(cur[name] - p)
						}
					}
					prev = cur
				case <-q.stop:
					return
				}
			}
		}()
	})
}

func (q *QuotaBalance) Close() error {
	q.stopOnce.Do(func() {
		if q.stop != nil {
			close(q.stop)
		}
	})
	return nil
}

func NewQuotaBalance(option GroupCommonOption, quotaOption QuotaBalanceOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*QuotaBalance, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	q := &QuotaBalance{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.QuotaBalance,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		testUrl: option.URL,
		quotas:  quotaOption.Quota,
	}
	q.startStats()
	return q, nil
}

func (q *QuotaBalance) Providers() []P.ProxyProvider { return q.providers }

func (q *QuotaBalance) Proxies() []C.Proxy { return q.GetProxies(false) }

func (q *QuotaBalance) SupportUDP() bool { return true }

func (q *QuotaBalance) IsL3Protocol(metadata *C.Metadata) bool { return true }
