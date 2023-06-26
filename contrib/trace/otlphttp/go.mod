module github.com/gogf/gf/contrib/trace/otlphttp/v2

go 1.15

require (
	github.com/gogf/gf/v2 v2.4.4
	go.opentelemetry.io/otel v1.16.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.16.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.16.0
	go.opentelemetry.io/otel/sdk v1.16.0
)

require (
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.15.2 // indirect
	golang.org/x/net v0.10.0 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
)

replace github.com/gogf/gf/v2 => ../../../
