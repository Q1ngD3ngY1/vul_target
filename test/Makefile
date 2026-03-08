#####################################################################
#                                                                   #
#  @(#)Makefile  March 27, 2024                                     #
#  Copyright(c) 2024, halelv@Tencent. All rights reserved.          #
#                                                                   #
#####################################################################

# 记录当前系统名
WORKING_OS := "$(shell uname -s)"

# Mac 系统名
OS_DARWIN := "Darwin"

# 兼容 Linux 和 Mac 命令
sed0 := sed

ifeq ($(WORKING_OS), $(OS_DARWIN))
	sed0 := gsed
endif

# 取 git 用户名优先
DUNGEON0 := $(shell git config --get user.name)
ifeq ($(DUNGEON0), "")
	DUNGEON0 := $(USER)
endif

# 编译出来的二进制版本号, 优先使用系统中的 $VERSION
BINARY_VERSION := $(if $(VERSION),$(VERSION),$(DUNGEON0)@$(shell date +'%Y.%m.%d.%H.%M.%S'))

default:
	@#if [ "$$(($$RANDOM % 10))" -eq 0 ]; then make stub; fi
	go mod tidy
	@$(sed0) -i "s/buildVersion *= *\"[^\"]*\"/buildVersion = \"${BINARY_VERSION}\"/g" cmd/server/main.go
	CGO_ENABLED=0 go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o bot-knowledge-config-server cmd/server/main.go
	@$(sed0) -i "s/buildVersion *= *\"[^\"]*\"/buildVersion = \"服务编译版本号, 勿动~~\"/g" cmd/server/main.go
	@./bot-knowledge-config-server --version

build-linux:
	@#if [ "$$(($$RANDOM % 10))" -eq 0 ]; then make stub; fi
	go mod tidy
	@$(sed0) -i "s/buildVersion *= *\"[^\"]*\"/buildVersion = \"${BINARY_VERSION}\"/g" cmd/server/main.go
	GOOS=linux GOARCH=amd64 go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o bot-knowledge-config-server cmd/server/main.go
	@$(sed0) -i "s/buildVersion *= *\"[^\"]*\"/buildVersion = \"服务编译版本号, 勿动~~\"/g" cmd/server/main.go

# golang format code
fmt:
	find . -name '*.go' -exec $(GOPATH)/bin/goimports -w -l {} \;
	@echo "OK"

# stub code and dependencies
stub:
	go get -u git.woa.com/dialogue-platform/go-comm@master
	go get -u git.woa.com/dialogue-platform/lke_proto@master
	go mod tidy

# staticcheck
# 官网: https://staticcheck.io/
# 安装: go install honnef.co/go/tools/cmd/staticcheck@latest
# golangci-lint
# 官网: https://golangci-lint.run/
# 安装: go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.41.1
lint:
	staticcheck ./...
	golangci-lint run

# 创建标准 TAG
# make tag: 从 master 打 TAG
# make tag FROM_BRANCH=true: 从当前分支打 TAG
tag:
	@./build/git-tag.sh $(FROM_BRANCH)

# upload to TKEx-CSIG
# DUNGEON=xxx, 可开启副本模式, 使用: DUNGEON=xxx make upload2tkex
upload2tkex:
	@./build/rtools-patch.sh

# 个人部署环境的节点发布
upload2tkex4self:
	@./build/rtools-patch.sh "-$(DUNGEON0)"

# shortcut for upload2tkex
ut: upload2tkex

# shortcut for upload2tkex4self
uts: upload2tkex4self


build-linux:
	@echo "build go binary for linux..."
	go mod tidy
	#GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry
	#.conflictPolicy=ignore" -o kb-parse-engine cmd/server/main.go
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o bot-knowledge-config-server cmd/server/main.go
	#GOOS=linux GOARCH=amd64 go build -o kb-parse-engine cmd/server/main.go
	@echo "build go binary for linux done"
