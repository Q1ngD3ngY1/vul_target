#!/bin/bash

# trpc-cli 相关资料
# https://iwiki.woa.com/p/194215409

#TRPC_CLI="$HOME/go/bin/trpc-cli"
TRPC_CLI="$HOME/gosrc0/trpc-cli"
if [ ! -f "$TRPC_CLI" ]; then
    TRPC_CLI="$HOME/gopath/bin/trpc-cli"
fi

PROTO_FILE="/Users/leyton/gosrc0/kep/lke_proto/thirds-pb/bot-knowledge-config-server/knowledge-config.proto"
if [ ! -f "$PROTO_FILE" ]; then
    PROTO_FILE="$HOME/work/lke_proto/thirds-pb/bot-knowledge-config-server/knowledge-config.proto"
fi

# trpc.KEP.bot-knowledge-config-server.Admin 端口号9090
# trpc.KEP.bot-knowledge-config-server.Login 端口号9091
# trpc.KEP.bot-knowledge-config-server.Api 端口号9092
#TARGET="ip://21.21.161.46:8000"
#TARGET="ip://21.24.118.77:8000"
TARGET="ip://21.21.125.156:9090"  # 根据service选择不同的端口号

echo "--------------------------------------------------------------"
echo "TRPC_CLI=$TRPC_CLI"
echo "PROTO_FILE=$PROTO_FILE"
echo "TARGET=$TARGET"
echo "--------------------------------------------------------------"


# $TRPC_CLI -protofile="$PROTO_FILE" -interfacelist

# $TRPC_CLI -protofile="$PROTO_FILE" -interfacename=GetUnreleasedCount -outputjson=GetUnreleasedCount.json
# $TRPC_CLI -protofile="$PROTO_FILE" -interfacename=SendDataSyncTaskEvent -outputjson=SendDataSyncTaskEvent.json


