package outboundgroup

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/metacubex/mihomo/common/atomic"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
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

// QuotaBalance 按连接数/带宽配额 + 健康状态的负载均衡组。
//
// 设计要点：
//   - 连接选择与计数在同一把锁内原子完成（selectAndReserve），并发连接不会同时
//     选到同一节点；
//   - 连接数使用实时预留计数（DialContext 时 +1，失败 -1），旁路统计每秒用
//     statistic.Manager 的真实连接数校正；
//   - 带宽/延迟来自旁路采样（独立 goroutine，不阻塞选择路径）；
//   - 组级健康检查按 interval 定时对节点做 URLTest，更新 alive/delay。
type QuotaBalance struct {
	*GroupBase
	testUrl        string
	testTimeout    int
	expectedStatus utils.IntRanges[uint16]
	quotas         map[string]QuotaRule
	stats          sync.Map // name -> *quotaStat（旁路统计：字节）
	loads          sync.Map // name -> *atomic.Int64（实时预留计数）
	mu             sync.Mutex // 保护「选择 + 预留」的原子性
	stopOnce       sync.Once
	stop           chan struct{}
	startOnce      sync.Once
	logLast        atomic.Int64
}

// loadOf 返回节点的实时预留计数。
func (q *QuotaBalance) loadOf(name string) *atomic.Int64 {
	if v, ok := q.loads.Load(name); ok {
		return v.(*atomic.Int64)
	}
	z := atomic.NewInt64(0)
	v, _ := q.loads.LoadOrStore(name, &z)
	return v.(*atomic.Int64)
}

// satScore 返回节点的实时饱和分数：连接数用实时预留计数，带宽用旁路统计。
func (q *QuotaBalance) satScore(name string) float64 {
	rule, ok := q.quotas[name]
	if !ok {
		return 0
	}
	var score float64
	if rule.MaxConnections > 0 {
		score += float64(q.loadOf(name).Load()) / float64(rule.MaxConnections)
	}
	if rule.MaxBandwidth > 0 {
		if v, ok := q.stats.Load(name); ok {
			st := v.(*quotaStat)
			score += float64(st.bytes.Load()) * 8 / 1e6 / float64(rule.MaxBandwidth)
		}
	}
	return score
}

// selectBest 是纯选择逻辑（不加锁、不计数）。
func (q *QuotaBalance) selectBest(metadata *C.Metadata, touch bool) C.Proxy {
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
		now := time.Now().UnixNano()
		last := q.logLast.Load()
		if now-last > int64(time.Second) && q.logLast.CompareAndSwap(last, now) {
			log.Warnln("[QuotaBalance] %s no healthy proxy, fail fast (proxies=%d)", q.Name(), len(proxies))
		}
		return nil
	}
	if len(healthy) == 1 {
		return healthy[0]
	}
	best := healthy[0]
	bestScore := math.MaxFloat64
	for _, proxy := range healthy {
		score := q.satScore(proxy.Name())
		if score < bestScore {
			best = proxy
			bestScore = score
		}
	}
	return best
}

// selectAndReserve 在锁内完成「选择 + 预留计数」，并发连接时不会全部选到同一节点。
func (q *QuotaBalance) selectAndReserve(metadata *C.Metadata, touch bool) C.Proxy {
	q.mu.Lock()
	defer q.mu.Unlock()
	proxy := q.selectBest(metadata, touch)
	if proxy != nil {
		load := q.loadOf(proxy.Name()).Add(1)
		q.logSelect(proxy, load)
	}
	return proxy
}

// logSelect 输出限流的选择日志（每秒最多一条），记录实际选择、实时预留计数与延迟。
func (q *QuotaBalance) logSelect(proxy C.Proxy, load int64) {
	now := time.Now().UnixNano()
	last := q.logLast.Load()
	if now-last > int64(time.Second) && q.logLast.CompareAndSwap(last, now) {
		bytes := int64(0)
		if v, ok := q.stats.Load(proxy.Name()); ok {
			bytes = v.(*quotaStat).bytes.Load()
		}
		log.Infoln("[QuotaBalance] %s -> %s (load=%d bytes=%d delay=%dms)", q.Name(), proxy.Name(), load, bytes, proxy.LastDelayForTestUrl(q.testUrl))
	}
}

// Unwrap 供组 API / 面板展示使用（不计数，实际选择与日志在 selectAndReserve）。
func (q *QuotaBalance) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	return q.selectBest(metadata, touch)
}

func (q *QuotaBalance) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	proxy := q.selectAndReserve(metadata, false)
	if proxy == nil {
		return nil, errors.New("no healthy proxy in quota-balance group")
	}
	conn, err := proxy.DialContext(ctx, metadata)
	if err != nil {
		q.loadOf(proxy.Name()).Add(-1)
		return nil, err
	}
	return conn, nil
}

func (q *QuotaBalance) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	proxy := q.selectAndReserve(metadata, false)
	if proxy == nil {
		return nil, errors.New("no healthy proxy in quota-balance group")
	}
	conn, err := proxy.ListenPacketContext(ctx, metadata)
	if err != nil {
		q.loadOf(proxy.Name()).Add(-1)
		return nil, err
	}
	return conn, nil
}

func (q *QuotaBalance) Now() string {
	proxy := q.selectBest(nil, false)
	if proxy == nil {
		return ""
	}
	return proxy.Name()
}

// startStats 启动旁路统计：每秒采样 statistic.Manager，统计真实连接数/字节，
// 并用真实连接数校正实时预留计数。
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
						// 用真实连接数校正实时预留计数
						q.loadOf(name).Store(count)
					}
					prev = cur
				case <-q.stop:
					return
				}
			}
		}()
	})
}

// startHealthCheck 定时对组内节点做 URLTest，更新 alive/delay。
func (q *QuotaBalance) startHealthCheck(interval int) {
	if interval <= 0 {
		interval = 300
	}
	go func() {
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				timeout := q.testTimeout
				if timeout <= 0 {
					timeout = 5
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
				q.GroupBase.URLTest(ctx, q.testUrl, q.expectedStatus)
				cancel()
			case <-q.stop:
				return
			}
		}
	}()
}

func (q *QuotaBalance) Close() error {
	q.stopOnce.Do(func() {
		if q.stop != nil {
			close(q.stop)
		}
	})
	return nil
}

func (q *QuotaBalance) Providers() []P.ProxyProvider { return q.providers }

func (q *QuotaBalance) Proxies() []C.Proxy { return q.GetProxies(false) }

func (q *QuotaBalance) SupportUDP() bool { return true }

func (q *QuotaBalance) IsL3Protocol(metadata *C.Metadata) bool { return true }

func NewQuotaBalance(option GroupCommonOption, quotaOption QuotaBalanceOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*QuotaBalance, error) {
	if emptyFallback == nil {
		return nil, errors.New("empty fallback proxy not exist")
	}
	expectedStatus, err := utils.NewUnsignedRanges[uint16](option.ExpectedStatus)
	if err != nil {
		return nil, err
	}
	testUrl := option.URL
	if testUrl == "" {
		testUrl = C.DefaultTestURL
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
		testUrl:        testUrl,
		testTimeout:    option.TestTimeout,
		expectedStatus: expectedStatus,
		quotas:         quotaOption.Quota,
	}
	q.startStats()
	q.startHealthCheck(option.Interval)
	return q, nil
}
