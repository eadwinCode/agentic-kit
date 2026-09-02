module github.com/eadwinCode/agentic-kit/examples/go-app

go 1.25.0

require (
	github.com/eadwinCode/agentic-kit/packages/go-agentenkit v0.0.0
	github.com/redis/go-redis/v9 v9.22.0
	github.com/zendev-sh/goai v0.10.0
	modernc.org/sqlite v1.57.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/ncruces/go-strftime v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	go.uber.org/atomic v1.11.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	modernc.org/libc v1.74.4 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/eadwinCode/agentic-kit/packages/go-agentenkit => ../../packages/go-agentenkit
