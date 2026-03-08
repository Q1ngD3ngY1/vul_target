#!/usr/bin/env bash

BINARY=bin/${SERVER_NAME}

# 定义需要编译的工具目录列表
tools=(
)

set -x
go mod tidy
echo "start build binary..."

#which wire
#[[ $? -eq 1 ]] && go install github.com/google/wire/cmd/wire@latest

#wire ./cmd/server
GOOS=linux go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn" -o ${BINARY} ${GOARGS} ./cmd/server && ls -l ${BINARY}

#wire ./cmd/tool
# 遍历 tools 数组中指定的工具目录，逐个编译工具
for tool_name in "${tools[@]}"; do
    tool_dir="./cmd/tool/${tool_name}"
    if [ -d "$tool_dir" ]; then
        BINARY_TOOL_NAME="bin/${SERVER_NAME}-${tool_name}"
        echo "Building tool: ${tool_name} -> ${BINARY_TOOL_NAME}"
        GOOS=linux go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=warn" -o ${BINARY_TOOL_NAME} ${GOARGS} ${tool_dir} && ls -l ${BINARY_TOOL_NAME}
    else
        echo "Warning: tool directory not found: ${tool_dir}"
    fi
done
echo "build binary done"
pwd && ls -l .
