#!/bin/bash

#####################################################################
#                                                                   #
#  @(#)rtools-patch.sh  March 27, 2024                              #
#  Copyright(c) 2024, halelv@Tencent. All rights reserved.          #
#                                                                   #
#####################################################################

# roots 下载地址, 下载后就安装
# https://mirrors.tencent.com/repository/generic/thcomm/rtools/rtools_darwin_link
# 安装时注意放行不信任源
# In System Preferences, click Security & Privacy, then click General.
# Click the lock and enter your password to make changes.
# Select App Store under the header “Allow apps downloaded from.”

# 记录当前系统名
WORKING_OS="$(uname -s)"
# CPU 架构
ARCH="$(uname -m)"

# Mac 系统名
OS_DARWIN="Darwin"
# linux
OS_LINUX="Linux"

# 兼容 Linux 和 Mac 命令
sed0="sed"

# 构建命令
RTOOLS="${HOME}/.tkexTools/bin/rtools_linux_v1.0.2"

if [ "$OS_DARWIN" == "$WORKING_OS" ]; then
  sed0="gsed"
  RTOOLS="${HOME}/.tkexTools/bin/rtools_darwin_v1.0.2"
fi

PATCH_USER="$(git config --get user.name)"
if [ -z "$PATCH_USER" ]; then
	PATCH_USER="$(USER)"
fi
APP="KEP"
ORIGIN_SERVER="bot-knowledge-config-server"
POLARIS_SERVANT="knowledge-config"

DUNGEON_SERVER="$ORIGIN_SERVER$1"
# $DUNGEON 来自环境变量
if [ -n "$DUNGEON" ]; then
  DUNGEON_SERVER="$ORIGIN_SERVER-$DUNGEON$1"
fi

ENV="dev"
echo "ORIGIN: ${APP}.${ORIGIN_SERVER}"
if [ "$ORIGIN_SERVER" != "$DUNGEON_SERVER" ]; then
  echo "DUNGEON: ${APP}.${DUNGEON_SERVER}"
fi

LANG="trpc-golang"
TMP_DIR="${ORIGIN_SERVER}_tmp_dir"
TGZ="${ORIGIN_SERVER}.tgz"

# 确保 mac os 编译出来的包是 linux/amd64
export GOOS="linux"
export GOARCH="amd64"

# 二进制版本
BINARY_VERSION="${PATCH_USER}@$(date +'%Y.%m.%d.%H.%M.%S')"
echo "BINARY_VERSION=$BINARY_VERSION"
"$sed0" -i "s/buildVersion *= *\"[^\"]*\"/buildVersion = \"${BINARY_VERSION}\"/g" cmd/server/main.go
if [ "$ORIGIN_SERVER" != "$DUNGEON_SERVER" ]; then
  "$sed0" -i "s/\"trpc.$APP.$ORIGIN_SERVER.$POLARIS_SERVANT\"/\"trpc.$APP.$DUNGEON_SERVER.$POLARIS_SERVANT\"/g" cmd/server/main.go
  # "$sed0" -i "s/trpc0.DungeonForServer(\"$ORIGIN_SERVER\")/trpc0.DungeonForServer(\"$DUNGEON_SERVER\")/g" main.go
fi

# 编译构建
go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o "$DUNGEON_SERVER" cmd/server/main.go
# go build result
gbr="$?"

# 还原
"$sed0" -i "s/buildVersion *= *\"[^\"]*\"/buildVersion = \"服务编译版本号, 勿动~~\"/g" cmd/server/main.go

if [ "$gbr" -ne 0 ]; then
  echo "编译失败, 请检查修复后重试~"
  exit
fi

# if [ -f "$TGZ" ]; then
#   mv -vf "$TGZ" "$SERVER.`date +%Y%m%d.%H%M`.tgz"
# fi

# 清除临时目录
rm -rf "$TMP_DIR"
# 打包命令
mkdir -p "$TMP_DIR"/bin/
mkdir -p "$TMP_DIR"/conf/
mkdir -p "$TMP_DIR"/script/

#
if [ -f .extra-tar-files ]; then
  for line in $(cat .extra-tar-files); do
    cp -r "$line" "$TMP_DIR"/bin/
  done
fi;

cp -rf "$DUNGEON_SERVER" "$TMP_DIR"/bin/
cd "$TMP_DIR" && tar -czvf ../"$TGZ" .
cd ../ && rm -rf "$TMP_DIR"

# 发布上传
"$RTOOLS" \
  -env "$ENV" \
  -app "$APP" \
  -server "$DUNGEON_SERVER" \
  -package "$TGZ" \
  -user "$PATCH_USER" \
  -servant "$POLARIS_SERVANT" \
  -instances "$INSTANCES" \
  -lang "$LANG"

if [ "$ORIGIN_SERVER" != "$DUNGEON_SERVER" ]; then
  "$sed0" -i "s/\"trpc.$APP.$DUNGEON_SERVER.$POLARIS_SERVANT\"/\"trpc.$APP.$ORIGIN_SERVER.$POLARIS_SERVANT\"/g" cmd/server/main.go
  # "$sed0" -i "s/trpc0.DungeonForServer(\"$DUNGEON_SERVER\")/trpc0.DungeonForServer(\"$ORIGIN_SERVER\")/g" main.go
fi

rm -rf "$TGZ"
