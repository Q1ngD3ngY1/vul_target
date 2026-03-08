module git.woa.com/adp/kb/kb-config

go 1.24.3

// 私有化上报使用, 不了解请勿动, 感谢~
replace git.code.oa.com/trpc-go/trpc-metrics-prometheus => git.woa.com/dialogue-platform/tools/trpc-metrics-prometheus v0.1.13

replace (
	github.com/gomodule/redigo => github.com/gomodule/redigo v1.8.9
	github.com/zishang520/engine.io-go-parser => github.com/TeCHiScy/engine.io-go-parser v1.2.4
	github.com/zishang520/socket.io-go-parser/v2 => github.com/TeCHiScy/socket.io-go-parser/v2 v2.0.5
)

require (
	git.code.oa.com/trpc-go/trpc-config-rainbow v0.6.0
	git.code.oa.com/trpc-go/trpc-database/gorm v0.3.0
	git.code.oa.com/trpc-go/trpc-database/localcache v0.1.16
	git.code.oa.com/trpc-go/trpc-database/mysql v0.3.0
	git.code.oa.com/trpc-go/trpc-database/timer v0.2.0
	git.code.oa.com/trpc-go/trpc-filter/slime v0.3.2
	git.code.oa.com/trpc-go/trpc-go v0.21.1
	git.code.oa.com/trpc-go/trpc-naming-polaris v0.5.27
	git.woa.com/adp/common/llm v0.0.0-20251002102132-f24c3d6290e5
	git.woa.com/adp/common/workflow v0.0.0-20251201104649-5f36ef6d1131
	git.woa.com/adp/common/x v0.0.0-20260119150921-83cb05af0242
	git.woa.com/adp/gorm-gen v0.0.0-20260108032050-bf40825e28ea
	git.woa.com/adp/pb-go v0.0.0-20260121031743-be7eb4c83c6b
	git.woa.com/dialogue-platform/bot-config/task_scheduler v0.1.1-0.20251113135106-9373d4a0864b
	git.woa.com/dialogue-platform/lke_proto v1.0.4-woa.0.20251013133459-99dc0703d984
	git.woa.com/dialogue-platform/llm/sdk/query-similarity v1.0.3
	git.woa.com/dialogue-platform/proto v0.1.101-0.20251207163656-18412995a5b6
	git.woa.com/ivy/protobuf/trpc-go/qbot/finance/finance v0.0.0-20251212064226-d6f891aa9fd5
	git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/infosec v0.0.0-20240730070510-f9d9a16e9d50
	git.woa.com/ivy/protobuf/trpc-go/qbot/qbot/short_url v0.0.0-20240730070510-f9d9a16e9d50
	git.woa.com/sec-api/go/scurl v0.3.0
	git.woa.com/tencentcloud-internal/tencentcloud-sdk-go/tencentcloud/common v1.0.745
	git.woa.com/trpc-go/trpc-database/goes v0.0.8
	git.woa.com/trpc-go/trpc-database/goredis/v3 v3.3.5
	github.com/JohannesKaufmann/html-to-markdown v1.6.0
	github.com/PuerkitoBio/goquery v1.10.3
	github.com/agiledragon/gomonkey/v2 v2.13.0
	github.com/avast/retry-go v3.0.0+incompatible
	github.com/bwmarrin/snowflake v0.3.0
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc
	github.com/denisenkom/go-mssqldb v0.12.3
	github.com/elastic/go-elasticsearch/v8 v8.19.0
	github.com/gabriel-vasile/mimetype v1.4.10
	github.com/glycerine/goconvey v0.0.0-20190410193231-58a59202ab31
	github.com/go-ego/gse v0.80.3
	github.com/go-shiori/go-readability v0.0.0-20250217085726-9f5bf5ca7612
	github.com/go-sql-driver/mysql v1.9.3
	github.com/gomarkdown/markdown v0.0.0-20231222211730-1d6d20845b47
	github.com/google/go-cmp v0.7.0
	github.com/google/uuid v1.6.0
	github.com/google/wire v0.7.0
	github.com/jedib0t/go-pretty/v6 v6.7.2
	github.com/json-iterator/go v1.1.12
	github.com/lib/pq v1.10.9
	github.com/looplab/fsm v1.0.3
	github.com/redis/go-redis/v9 v9.14.1
	github.com/russross/blackfriday/v2 v2.1.0
	github.com/sergi/go-diff v1.4.0
	github.com/shopspring/decimal v1.4.0
	github.com/sijms/go-ora/v2 v2.9.0
	github.com/spf13/cast v1.10.0
	github.com/spf13/cobra v1.8.1
	github.com/stretchr/testify v1.11.1
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/common v1.1.11
	github.com/tencentcloud/tencentcloud-sdk-go/tencentcloud/sts v1.1.11
	github.com/tencentyun/cos-go-sdk-v5 v0.7.71
	github.com/tencentyun/qcloud-cos-sts-sdk v0.0.0-20250515025012-e0eec8a5d123
	github.com/wcharczuk/go-chart/v2 v2.1.2
	github.com/xuri/excelize/v2 v2.9.1
	go.opentelemetry.io/otel/trace v1.38.0
	golang.org/x/exp v0.0.0-20250819193227-8b4c13bb791b
	golang.org/x/net v0.44.0
	golang.org/x/sync v0.18.0
	golang.org/x/text v0.30.0
	google.golang.org/protobuf v1.36.10
	gopkg.in/yaml.v2 v2.4.0
	gorm.io/gen v0.3.27
	gorm.io/gorm v1.31.1
)

require (
	go.opentelemetry.io/otel v1.38.0
	go.opentelemetry.io/otel/sdk v1.37.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	filippo.io/edwards25519 v1.1.0 // indirect
	git.code.oa.com/devsec/protoc-gen-secv v0.3.4 // indirect
	git.code.oa.com/polaris/polaris-go v0.12.16 // indirect
	git.code.oa.com/rainbow/golang-sdk v0.6.2 // indirect
	git.code.oa.com/rainbow/proto v1.124.0 // indirect
	git.code.oa.com/tpstelemetry/trpc-gc-tuning-plugin v0.5.3 // indirect
	git.code.oa.com/trpc-go/trpc v0.1.2 // indirect
	git.code.oa.com/trpc-go/trpc-database/cos v0.1.6 // indirect
	git.code.oa.com/trpc-go/trpc-database/goredis v0.3.8 // indirect
	git.code.oa.com/trpc-go/trpc-filter/debuglog v0.1.14 // indirect
	git.code.oa.com/trpc-go/trpc-filter/recovery v0.1.5 // indirect
	git.code.oa.com/trpc-go/trpc-filter/validation v0.1.3 // indirect
	git.code.oa.com/trpc-go/trpc-metrics-prometheus v0.2.0 // indirect
	git.code.oa.com/trpc-go/trpc-metrics-runtime v0.5.22 // indirect
	git.code.oa.com/trpc-go/trpc-overload-control v1.4.44 // indirect
	git.code.oa.com/trpc-go/trpc-selector-dsn v0.2.1 // indirect
	git.code.oa.com/trpc-go/trpc-utils v0.2.3-0.20250718045050-23d95196ca39 // indirect
	git.code.oa.com/trpc-go/trpc-utils/robust/codec v0.2.1-0.20250718045050-23d95196ca39 // indirect
	git.woa.com/galileo/eco/go/sdk/base v0.23.0 // indirect
	git.woa.com/galileo/trpc-go-galileo v0.23.0 // indirect
	git.woa.com/jce/jce v1.2.0 // indirect
	git.woa.com/opentelemetry/opentelemetry-go-ecosystem v0.6.3 // indirect
	git.woa.com/opentelemetry/opentelemetry-go-ecosystem/instrumentation/oteltrpc v0.6.0 // indirect
	git.woa.com/polaris/polaris-go/v2 v2.6.10 // indirect
	git.woa.com/polaris/polaris-server-api/api/metric v1.0.2 // indirect
	git.woa.com/polaris/polaris-server-api/api/monitor v1.0.8 // indirect
	git.woa.com/polaris/polaris-server-api/api/v1/grpc v1.0.2 // indirect
	git.woa.com/polaris/polaris-server-api/api/v1/model v1.2.6 // indirect
	git.woa.com/polaris/polaris-server-api/api/v2/grpc v1.0.0 // indirect
	git.woa.com/polaris/polaris-server-api/api/v2/model v1.0.7 // indirect
	git.woa.com/sec-api/go/checkurl v0.2.0 // indirect
	git.woa.com/ssmsdk/ssm-sdk-golang/rotated_credential v1.0.6 // indirect
	git.woa.com/ssmsdk/ssm-sdk-golang/ssm v1.1.0 // indirect
	git.woa.com/tpstelemetry/cgroups v0.2.3 // indirect
	git.woa.com/tpstelemetry/cgroups/cgroupsv2 v0.2.3 // indirect
	git.woa.com/tpstelemetry/tpstelemetry-protocol v0.0.2-0.20230403124315-f383964b6bcc // indirect
	git.woa.com/trpc-go/go_reuseport v1.7.0 // indirect
	git.woa.com/trpc-go/tnet v0.1.2 // indirect
	git.woa.com/trpc/trpc-robust/go-sdk v0.0.1 // indirect
	git.woa.com/trpc/trpc-robust/proto/pb/go/trpc-robust v0.0.0-20241120021538-8dfb323d8c12 // indirect
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/ClickHouse/ch-go v0.67.0 // indirect
	github.com/ClickHouse/clickhouse-go/v2 v2.40.1 // indirect
	github.com/RussellLuo/timingwheel v0.0.0-20191022104228-f534fd34a762 // indirect
	github.com/agnivade/levenshtein v1.2.1 // indirect
	github.com/alphadose/haxmap v1.4.1 // indirect
	github.com/andybalholm/brotli v1.2.0 // indirect
	github.com/andybalholm/cascadia v1.3.3 // indirect
	github.com/araddon/dateparse v0.0.0-20210429162001-6b43995a97de // indirect
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
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.3.2 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/ebitengine/purego v0.8.4 // indirect
	github.com/elastic/elastic-transport-go/v8 v8.7.0 // indirect
	github.com/elastic/go-elasticsearch/v7 v7.17.7 // indirect
	github.com/fsnotify/fsnotify v1.9.0 // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/gin-gonic/gin v1.10.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.7.1 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/go-playground/form/v4 v4.2.1 // indirect
	github.com/go-playground/validator/v10 v10.27.0 // indirect
	github.com/go-redis/redis/v8 v8.11.5 // indirect
	github.com/go-shiori/dom v0.0.0-20230515143342-73569d674e1c // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/gogs/chardet v0.0.0-20211120154057-b7413eaefb8f // indirect
	github.com/golang-sql/civil v0.0.0-20220223132316-b832511892a9 // indirect
	github.com/golang-sql/sqlexp v0.1.0 // indirect
	github.com/golang/freetype v0.0.0-20170609003504-e2365dfdc4a0 // indirect
	github.com/golang/mock v1.6.0 // indirect
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
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20231201235250-de7065d80cb9 // indirect
	github.com/jackc/pgx/v5 v5.5.5 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/jinzhu/copier v0.4.0 // indirect
	github.com/jinzhu/inflection v1.0.0 // indirect
	github.com/jinzhu/now v1.1.5 // indirect
	github.com/jmoiron/sqlx v1.4.0 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/jxskiss/base62 v1.1.0 // indirect
	github.com/kelindar/bitmap v1.5.3 // indirect
	github.com/kelindar/simd v1.1.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/klauspost/crc32 v1.3.0 // indirect
	github.com/lestrrat-go/strftime v1.1.1 // indirect
	github.com/lufia/plan9stats v0.0.0-20250317134145-8bc96cf8fc35 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/minio/crc64nvme v1.1.0 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/minio-go/v7 v7.0.97 // indirect
	github.com/mitchellh/go-homedir v1.1.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mozillazg/go-httpheader v0.4.0 // indirect
	github.com/mozillazg/go-pinyin v0.18.0 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/nanmu42/limitio v1.0.0 // indirect
	github.com/natefinch/lumberjack v2.0.0+incompatible // indirect
	github.com/panjf2000/ants/v2 v2.11.3 // indirect
	github.com/paulmach/orb v0.11.1 // indirect
	github.com/pelletier/go-toml/v2 v2.2.4 // indirect
	github.com/pganalyze/pg_query_go/v4 v4.2.3 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.22 // indirect
	github.com/pingcap/errors v0.11.5-0.20250523034308-74f78ae071ee // indirect
	github.com/pingcap/failpoint v0.0.0-20240528011301-b51a646c7c86 // indirect
	github.com/pingcap/log v1.1.0 // indirect
	github.com/pingcap/tidb/pkg/parser v0.0.0-20250925094337-7324a09904e7 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/prometheus/client_golang v1.23.0 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.65.0 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/qianbin/directcache v0.9.7 // indirect
	github.com/r3labs/sse/v2 v2.10.0 // indirect
	github.com/remeh/sizedwaitgroup v1.0.0 // indirect
	github.com/richardlehane/mscfb v1.0.4 // indirect
	github.com/richardlehane/msoleps v1.0.4 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/robfig/cron v1.2.0 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sebdah/goldie/v2 v2.5.5 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shirou/gopsutil/v3 v3.24.5 // indirect
	github.com/shirou/gopsutil/v4 v4.25.5 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	github.com/tidwall/gjson v1.18.0 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tiendc/go-deepcopy v1.6.0 // indirect
	github.com/tinylib/msgp v1.3.0 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.3.0 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.65.0 // indirect
	github.com/vcaesar/cedar v0.20.2 // indirect
	github.com/xuri/efp v0.0.1 // indirect
	github.com/xuri/nfp v0.0.1 // indirect
	github.com/yuin/goldmark v1.7.8 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
	go.etcd.io/etcd/api/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.9 // indirect
	go.etcd.io/etcd/client/v3 v3.5.9 // indirect
	go.opentelemetry.io/auto/sdk v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/host v0.53.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.53.0 // indirect
	go.opentelemetry.io/contrib/zpages v0.53.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v1.28.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.37.0 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.37.0 // indirect
	go.opentelemetry.io/otel/metric v1.38.0 // indirect
	go.opentelemetry.io/otel/sdk/metric v1.37.0 // indirect
	go.opentelemetry.io/proto/otlp v1.7.1 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/automaxprocs v1.6.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	golang.org/x/arch v0.20.0 // indirect
	golang.org/x/crypto v0.42.0 // indirect
	golang.org/x/image v0.28.0 // indirect
	golang.org/x/mod v0.28.0 // indirect
	golang.org/x/sys v0.36.0 // indirect
	golang.org/x/tools v0.37.0 // indirect
	google.golang.org/genproto v0.0.0-20230526203410-71b5a4ffd15e // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250811230008-5f3141c8851a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250811230008-5f3141c8851a // indirect
	google.golang.org/grpc v1.74.2 // indirect
	gopkg.in/cenkalti/backoff.v1 v1.1.0 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.2.1 // indirect
	gorm.io/datatypes v1.2.7 // indirect
	gorm.io/driver/mysql v1.6.0 // indirect
	gorm.io/driver/postgres v1.5.2 // indirect
	gorm.io/hints v1.1.2 // indirect
	gorm.io/plugin/dbresolver v1.6.2 // indirect
)
