module github.com/gogf/gf/contrib/rpc/grpcx/v2

go 1.15

require (
	github.com/gogf/gf/contrib/registry/file/v2 v2.4.3
	github.com/gogf/gf/v2 v2.4.3
	go.opentelemetry.io/otel v1.13.0
	go.opentelemetry.io/otel/trace v1.13.0
	google.golang.org/grpc v1.49.0
	google.golang.org/protobuf v1.28.1
)

replace (
	github.com/gogf/gf/contrib/registry/file/v2 => ../../registry/file/
	github.com/gogf/gf/v2 => ../../../
)
