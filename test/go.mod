// KEP.bot-knowledge-config-server
//
// @(#)go.mod  March 27, 2024
// Copyright(c) 2024, halelv@Tencent. All rights reserved.

module git.woa.com/dialogue-platform/bot-config/bot-knowledge-config-server

go 1.24.3

// 私有化上报使用, 不了解请勿动, 感谢~
replace git.code.oa.com/trpc-go/trpc-metrics-prometheus => git.woa.com/dialogue-platform/tools/trpc-metrics-prometheus v0.1.13

require (
	git.woa.com/dialogue-platform/go-comm v0.2.28-0.20250904161057-9be638b0f744
	git.woa.com/dialogue-platform/lke_proto v1.0.4-woa.0.20251026102612-472c10835807
)

require (
	git.woa.com/dialogue-platform/bot-config/task_scheduler v0.1.1-0.20250914094108-f526d7b91f2d
	git.woa.com/dialogue-platform/proto v0.1.98-0.20250924132830-debb389384f5
	git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/chat v0.0.0-20250513110956-11ce0dccc3ae
	git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec v0.0.0-20240730070510-f9d9a16e9d50
	git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url v0.0.0-20240730070510-f9d9a16e9d50
)

require (
	git.code.oa.com/trpc-go/trpc-database/cos v0.1.6
	git.code.oa.com/trpc-go/trpc-database/goredis v0.3.7
	git.code.oa.com/trpc-go/trpc-database/gorm v0.3.0
	git.code.oa.com/trpc-go/trpc-database/localcache v0.1.16
	git.code.oa.com/trpc-go/trpc-database/mysql v0.3.0
	git.code.oa.com/trpc-go/trpc-database/redis v0.2.3
	git.code.oa.com/trpc-go/trpc-database/timer v0.2.0
	git.code.oa.com/trpc-go/trpc-filter/slime v0.3.2
	git.code.oa.com/trpc-go/trpc-go v0.21.1
	git.code.oa.com/trpc-go/trpc-metrics-prometheus v0.2.0
	git.woa.com/adp/common/llm v0.0.0-20250830071031-438b5c07d321
	git.woa.com/adp/common/x v0.0.0-20251015144119-3eb1450c0d68
	git.woa.com/baicaoyuan/moss v0.13.20-0.20250828033403-141124264f91
	git.woa.com/dialogue-platform/common/v3 v3.0.10-0.20250723092044-05c48d15324b
	git.woa.com/dialogue-platform/llm/sdk/query-similarity v1.0.3
	git.woa.com/dialogue-platform/yuanqi/yuanqi-naming-polaris v0.0.8
	git.woa.com/galileo/trpc-go-galileo v0.23.0
	git.woa.com/ivy/protobuf/global-pb-server v1.0.87268864
	git.woa.com/ivy/protobuf/inner-to-trpc v1.0.76666834
	git.woa.com/ivy/protobuf/trpc-go/qbot/finance/finance v0.0.0-20250513081647-38406c434b23
	git.woa.com/polaris/polaris-go/v2 v2.6.10
	git.woa.com/sec-api/go/scurl v0.3.0
	git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common v1.0.745
	git.woa.com/trpc-go/trpc-database/goes v0.0.6
	git.woa.com/trpc-go/trpc-database/goredis/v3 v3.3.5
	github.com/JohannesKaufmann/html-to-markdown v1.4.0
	github.com/PuerkitoBio/goquery v1.8.1
	github.com/agiledragon/gomonkey/v2 v2.13.0
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/bwmarrin/snowflake v0.3.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/elastic/go-elasticsearch/v8 v8.13.0
	github.com/gabriel-vasile/mimetype v1.4.3
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31
	github.com/go-ego/gse v0.80.2
	github.com/go-redis/redis/v8 v8.11.5
	github.com/go-shiori/go-readability v0.0.0-20230421032831-c66949dfc0ad
	github.com/go-sql-driver/mysql v1.9.3
	github.com/golang/mock v1.6.0
	github.com/gomarkdown/markdown v0.0.0-20231222211730-1d6d20845b47
	github.com/gomodule/redigo v2.0.0+incompatible
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/jmoiron/sqlx v1.4.0
	github.com/json-iterator/go v1.1.12
	github.com/looplab/fsm v1.0.2
	github.com/minio/minio-go/v7 v7.0.95
	github.com/russross/blackfriday/v2 v2.1.0
	github.com/sergi/go-diff v1.3.1
	github.com/shopspring/decimal v1.4.0
	github.com/spf13/cast v1.10.0
	github.com/stretchr/testify v1.11.1
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common v1.1.11
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts v1.1.11
	github.com/tencentyun/cos-go-sdk-v5 v0.7.70
	github.com/tencentyun/qcloud-cos-sts-sdk v0.0.0-20250515025012-e0eec8a5d123
	github.com/wcharczuk/go-chart/v2 v2.1.2
	github.com/xuri/excelize/v2 v2.8.0
	go.opentelemetry.io/otel/trace v1.38.0
	golang.org/x/exp v0.0.0-20250819193227-8b4c13bb791b
	golang.org/x/net v0.43.0
	golang.org/x/sync v0.17.0
	golang.org/x/text v0.30.0
	google.golang.org/protobuf v1.36.10
	gopkg.in/yaml.v3 v3.0.1
	gorm.io/driver/mysql v1.6.0
	gorm.io/gorm v1.31.0
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	git.code.oa.com/atta/attaapi-go v1.0.8 // indirect
	git.code.oa.com/cm-metrics/sdk_metric_pb v0.0.6 // indirect
	git.code.oa.com/cm-metrics/tccm-client-go v1.2.2 // indirect
	git.code.oa.com/cm-metrics/trpc-metrics-tccm-go v1.30.2 // indirect
	git.code.oa.com/devsec/protoc-gen-secv v0.3.4 // indirect
	git.code.oa.com/pcgmonitor/trpc_report_api_go v0.3.13 // indirect
	git.code.oa.com/polaris/polaris-go v0.12.16 // indirect
	git.code.oa.com/rainbow/golang-sdk v0.6.2 // indirect
	git.code.oa.com/rainbow/proto v1.124.0 // indirect
	git.code.oa.com/tpstelemetry/trpc-gc-tuning-plugin v0.5.3 // indirect
	git.code.oa.com/trpc-go/trpc v0.1.2 // indirect
	git.code.oa.com/trpc-go/trpc-config-rainbow v0.6.0 // indirect
	git.code.oa.com/trpc-go/trpc-filter/debuglog v0.1.14 // indirect
	git.code.oa.com/trpc-go/trpc-filter/recovery v0.1.5 // indirect
	git.code.oa.com/trpc-go/trpc-filter/validation v0.1.3 // indirect
	git.code.oa.com/trpc-go/trpc-log-atta v0.2.0 // indirect
	git.code.oa.com/trpc-go/trpc-metrics-m007 v0.5.1 // indirect
	git.code.oa.com/trpc-go/trpc-metrics-runtime v0.5.21 // indirect
	git.code.oa.com/trpc-go/trpc-naming-polaris v0.5.27 // indirect
	git.code.oa.com/trpc-go/trpc-overload-control v1.4.42 // indirect
	git.code.oa.com/trpc-go/trpc-selector-dsn v0.2.1 // indirect
	git.code.oa.com/trpc-go/trpc-utils v0.2.3-0.20250718045050-23d95196ca39 // indirect
	git.code.oa.com/trpc-go/trpc-utils/robust/codec v0.2.1-0.20250718045050-23d95196ca39 // indirect
	git.woa.com/baicaoyuan/apex/proto v0.0.1 // indirect
	git.woa.com/galileo/eco/go/sdk/base v0.23.0 // indirect
	git.woa.com/jce/jce v1.2.0 // indirect
	git.woa.com/opentelemetry/opentelemetry-go-ecosystem v0.6.3 // indirect
	git.woa.com/opentelemetry/opentelemetry-go-ecosystem/instrumentation/oteltrpc v0.6.3 // indirect
	git.woa.com/polaris/polaris-server-api/api/metric v1.0.2 // indirect
	git.woa.com/polaris/polaris-server-api/api/monitor v1.0.8 // indirect
	git.woa.com/polaris/polaris-server-api/api/v1/grpc v1.0.2 // indirect
	git.woa.com/polaris/polaris-server-api/api/v1/model v1.2.6 // indirect
	git.woa.com/polaris/polaris-server-api/api/v2/grpc v1.0.0 // indirect
	git.woa.com/polaris/polaris-server-api/api/v2/model v1.0.7 // indirect
	git.woa.com/ssmsdk/ssm-sdk-golang/rotated_credential v1.0.7 // indirect
	git.woa.com/ssmsdk/ssm-sdk-golang/ssm v1.1.0 // indirect
	git.woa.com/tpstelemetry/cgroups v0.2.3 // indirect
	git.woa.com/tpstelemetry/cgroups/cgroupsv2 v0.2.3 // indirect
	git.woa.com/tpstelemetry/tpstelemetry-protocol v0.0.2-0.20230403124315-f383964b6bcc // indirect
	git.woa.com/trpc-go/go_reuseport v1.7.0 // indirect
	git.woa.com/trpc-go/tnet v0.1.2 // indirect
	git.woa.com/trpc-go/trpc-metrics-zhiyan/v2 v2.1.5 // indirect
	git.woa.com/trpc/trpc-robust/go-sdk v0.0.1 // indirect
	git.woa.com/trpc/trpc-robust/proto/pb/go/trpc-robust v0.0.0-20241120021538-8dfb323d8c12 // indirect
	git.woa.com/zhiyan-monitor/sdk/go-sdk/v3 v3.3.6 // indirect
	git.woa.com/zhiyan-monitor/t-digest v0.0.6 // indirect
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/ClickHouse/ch-go v0.67.0 // indirect
	github.com/ClickHouse/clickhouse-go/v2 v2.40.1 // indirect
	github.com/IBM/sarama v1.45.2 // indirect
	github.com/RussellLuo/timingwheel v0.0.0-20191022104228-f534fd34a762 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/alphadose/haxmap v1.4.1 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/bytedance/gopkg v0.1.3 // indirect
	github.com/bytedance/sonic v1.14.1 // indirect
	github.com/bytedance/sonic/loader v0.3.0 // indirect
	github.com/cenkalti/backoff/v4 v4.3.0 // indirect
	github.com/cenkalti/backoff/v5 v5.0.3 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/clbanning/mxj v1.8.4 // indirect
	github.com/cloudwego/base64x v0.1.6 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/eapache/go-resiliency v1.7.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20230731223053-c322873962e3 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.5.0 // indirect
	github.com/elastic/go-elasticsearch/v7 v7.17.7 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/gin-gonic/gin v1.10.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-playground/form/v4 v4.2.1 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.20.0 // indirect
	github.com/go-shiori/dom v0.0.0-20210627111528-4e4722cd0d65 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gogs/chardet v0.0.0-20211120154057-b7413eaefb8f // indirect
	github.com/golang-sql/civil v0.0.0-20190719163853-cb61b32ac6fe // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/golang/snappy v1.0.0 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/pprof v0.0.0-20250630185457-6e76a2b096b5 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v1.2.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.16.0 // indirect
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.27.1 // indirect
	github.com/guillermo/go.procmeminfo v0.0.0-20131127224636-be4355a9fb0e // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/go-uuid v1.0.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/pgx/v5 v5.3.1 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.7.6 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.4 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jinzhu/copier v0.4.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/jxskiss/base62 v1.1.0 // indirect
	github.com/kelindar/bitmap v1.5.3 // indirect
	github.com/kelindar/simd v1.1.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lestrrat-go/strftime v1.1.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20250317134145-8bc96cf8fc35 // indirect
	github.com/martinlindhe/base36 v1.1.1 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/minio/crc64nvme v1.0.2 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826 // indirect
	github.com/mozillazg/go-httpheader v0.4.0 // indirect
	github.com/mozillazg/go-pinyin v0.18.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nanmu42/limitio v1.0.0 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/panjf2000/ants/v2 v2.11.3 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pingcap/errors v0.11.5-0.20221009092201-b66cddb77c32 // indirect
	github.com/pingcap/failpoint v0.0.0-20220801062533-2eaa32854a6c // indirect
	github.com/pingcap/log v1.1.1-0.20230317032135-a0d097d16e22 // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20231124053542-069631e2ecfe // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.23.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.65.0 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/qianbin/directcache v0.9.7 // indirect
	github.com/r3labs/sse/v2 v2.10.0 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	github.com/redis/go-redis/v9 v9.14.0 // indirect
	github.com/remeh/sizedwaitgroup v1.0.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/richardlehane/mscfb v1.0.4 // indirect
	github.com/richardlehane/msoleps v1.0.3 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sebdah/goldie/v2 v2.5.5 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shirou/gopsutil/v4 v4.25.5 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tinylib/msgp v1.3.0 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.65.0 // indirect
	github.com/vcaesar/cedar v0.20.1 // indirect
	github.com/xuri/efp v0.0.0-20230802181842-ad255f2331ca // indirect
	github.com/xuri/nfp v0.0.0-20230819163627-dc951e3ffe1a // indirect
	github.com/yuin/goldmark v1.7.8 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/etcd/api/v3 v3.5.10 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.10 // indirect
	go.etcd.io/etcd/client/v3 v3.5.10 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/host v0.53.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.53.0 // indirect
	go.opentelemetry.io/contrib/zpages v0.53.0 // indirect
	go.opentelemetry.io/otel v1.38.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.28.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk v1.37.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.opentelemetry.io/proto/otlp v1.7.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/arch v0.20.0 // indirect
	golang.org/x/crypto v0.41.0 // indirect
	golang.org/x/image v0.28.0 // indirect
	golang.org/x/sys v0.35.0 // indirect
	google.golang.org/genproto v0.0.0-20240227224415-6ceb2ff114de // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250811230008-5f3141c8851a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250811230008-5f3141c8851a // indirect
	google.golang.org/grpc v1.74.2 // indirect
	gopkg.in/cenkalti/backoff.v1 v1.1.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gorm.io/driver/postgres v1.5.2 // indirect
)

replace (
	github.com/gomodule/redigo => github.com/gomodule/redigo v1.8.9
	github.com/zishang520/engine.io-go-parser => github.com/TeCHiScy/engine.io-go-parser v1.2.4
	github.com/zishang520/socket.io-go-parser/v2 => github.com/TeCHiScy/socket.io-go-parser/v2 v2.0.5
)
