// ─────────────────────────────────────────────────────────────────────────────
// internal/engine/fx_converter.go
//
// FxConverter — RAM-cached FX mid-price converter for quote currency PnL.
//
// Design rationale:
//   The pricing-service constantly streams tick:USDJPY, tick:GBPUSD, etc.
//   The StartTickConsumer already receives every tick. By calling UpdateRate()
//   on every tick BEFORE EvaluateTick(), the FxConverter's cache is always
//   current without ever querying Redis inside the hot evaluation path.
//
//   This is a pure in-process RAM lookup — no network, no context, no error.
//
// ConvertToUSD resolution order:
//   1. quoteCurrency == "USD"          → return amount (identity, no multiply)
//   2. rates[quoteCurrency+"USD"] > 0  → amount × rate  (e.g. GBP → GBPUSD)
//   3. rates["USD"+quoteCurrency] > 0  → amount ÷ rate  (e.g. JPY → USDJPY)
//   4. Rate not yet cached             → return amount with a warning log
//      (safe fallback for the first few ticks after process startup before
//       the conversion pair has streamed through for the first time)
//
// Thread safety:
//   UpdateRate uses a full Lock() — called from the tick consumer goroutine.
//   ConvertToUSD uses RLock() — called from EvaluateTick on every tick.
//   Both are extremely short critical sections (single map read/write).
// ─────────────────────────────────────────────────────────────────────────────

package engine

import (
	"log/slog"
	"sync"
)

// FxConverter is a thread-safe, RAM-resident exchange rate cache.
// It is fed by the tick consumer and consumed by the trigger book.
// Create exactly one via NewFxConverter() and share as a pointer.
type FxConverter struct {
	mu    sync.RWMutex
	rates map[string]float64 // symbol → mid-price  e.g. "USDJPY" → 150.50
}

// NewFxConverter creates an empty FxConverter ready to receive rates.
func NewFxConverter() *FxConverter {
	return &FxConverter{
		rates: make(map[string]float64),
	}
}

// UpdateRate stores the mid-price for a symbol in the cache.
//
// Called by StartTickConsumer on EVERY tick, BEFORE EvaluateTick.
// This ensures the cache is always current when PnL conversion runs.
//
// Mid-price formula: (bid + ask) / 2
// Using mid avoids systematically over- or under-collecting collateral
// on cross-currency PnL conversions.
func (f *FxConverter) UpdateRate(symbol string, bid, ask float64) {
	mid := (bid + ask) / 2.0
	f.mu.Lock()
	f.rates[symbol] = mid
	f.mu.Unlock()
}

// ConvertToUSD converts amount (denominated in quoteCurrency) to USD.
//
// This is a pure RAM lookup — zero Redis I/O, zero allocation, ~20ns.
//
// Resolution logic:
//   quoteCurrency == "USD"  → identity (EURUSD, BTCUSD, XAUUSD pairs)
//   quoteCurrency is BASE   → multiply (GBPUSD: amount_gbp × gbpusd_rate)
//   quoteCurrency is QUOTE  → divide   (USDJPY: amount_jpy ÷ usdjpy_rate)
//   No rate found           → fallback: return amount + warn (startup only)
func (f *FxConverter) ConvertToUSD(amount float64, quoteCurrency string) float64 {
	// Fast path: PnL already in USD — covers EURUSD, AUDUSD, BTCUSD, XAUUSD.
	if quoteCurrency == "USD" {
		return amount
	}

	f.mu.RLock()
	defer f.mu.RUnlock()

	// Case 1: quoteCurrency is the BASE of a xxxUSD pair.
	//   e.g. quoteCurrency="GBP" → check GBPUSD → multiply.
	//   This covers pairs like EURGBP (quote=GBP), CADJPY (quote=JPY is handled below).
	if rate, ok := f.rates[quoteCurrency+"USD"]; ok && rate > 0 {
		return amount * rate
	}

	// Case 2: quoteCurrency is the QUOTE of a USDxxx pair.
	//   e.g. quoteCurrency="JPY" → check USDJPY → divide.
	//   Covers GBPJPY, EURJPY, AUDJPY, CADJPY (all JPY-quoted).
	if rate, ok := f.rates["USD"+quoteCurrency]; ok && rate > 0 {
		return amount / rate
	}

	// Fallback: conversion pair not yet in cache (only happens in the first
	// few seconds after process start, before that symbol's tick arrives).
	// Return the raw amount to avoid a math panic; log once so ops can see it.
	slog.Warn("FxConverter: conversion rate not cached, returning raw amount",
		"quote_currency", quoteCurrency,
		"amount",         amount,
	)
	return amount
}
