package config

import "sync"

type GlobalRiskParams struct {
	WarningThreshold float64
	StopOutThreshold float64
}

var (
	liveParams = GlobalRiskParams{
		WarningThreshold: 80.0,
		StopOutThreshold: 50.0,
	}
	mu sync.RWMutex
)

// GetParams securely retrieves the current risk thresholds using an RLock
func GetParams() GlobalRiskParams {
	mu.RLock()
	defer mu.RUnlock()
	return liveParams
}

// UpdateParams safely mutations the active risk parameters via Lock (e.g., from Kafka config stream)
func UpdateParams(warning, stopOut float64) {
	mu.Lock()
	defer mu.Unlock()
	liveParams.WarningThreshold = warning
	liveParams.StopOutThreshold = stopOut
}
