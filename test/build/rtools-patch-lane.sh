#!/bin/bash

# rtools 修改版（支持指定北极星Namespace） 下载地址, 下载后就安装对应操作系统的rtools即可
# https://mirrors.tencent.com/repository/generic/adp/ut/rtools/
# 安装时注意放行不信任源
# In System Preferences, click Security & Privacy, then click General.
# Click the lock and enter your password to make changes.
# Select App Store under the header "Allow apps downloaded from."

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
RTOOLS="${HOME}/.tkexTools/bin/rtools_linux_v1.0.8"

if [ "$OS_DARWIN" == "$WORKING_OS" ]; then
  sed0="gsed"
  RTOOLS="${HOME}/.tkexTools/bin/rtools_darwin_v1.0.8"
fi

# 检查并重新下载损坏的RTOOLS文件
if [ -e $RTOOLS ]; then
  echo "RTOOLS PATH: ${RTOOLS}"
  # 检查文件是否损坏，根据操作系统使用不同的格式检测
  if [ "$OS_DARWIN" == "$WORKING_OS" ]; then
    # macOS系统检测Mach-O格式
    if file "$RTOOLS" | grep -q "Mach-O"; then
      echo "RTOOLS文件格式正常"
    else
      echo "检测到RTOOLS文件可能损坏，重新下载..."
      rm -f "$RTOOLS"
    fi
  else
    # Linux系统检测ELF格式
    if file "$RTOOLS" | grep -q "ELF"; then
      echo "RTOOLS文件格式正常"
    else
      echo "检测到RTOOLS文件可能损坏，重新下载..."
      rm -f "$RTOOLS"
    fi
  fi
else
  echo "RTOOLS不存在，开始下载..."
fi

if [ ! -e $RTOOLS ]; then
  mkdir -p ${HOME}/.tkexTools/bin/
  if [ "$OS_DARWIN" == "$WORKING_OS" ]; then
    if ! command -v wget &> /dev/null; then
      brew install wget
    fi
    echo "下载Mac版RTOOLS..."
    wget 'https://mirrors.tencent.com/repository/generic/adp/ut/rtools/rtools_darwin_v1.0.8' -O $RTOOLS
  else
    echo "下载Linux版RTOOLS..."
    wget 'https://mirrors.tencent.com/repository/generic/adp/ut/rtools/rtools_linux_v1.0.8' -O $RTOOLS
  fi
  chmod +x $RTOOLS
  echo "RTOOLS下载完成并设置执行权限"
fi

PATCH_USER="$(git config --get user.name)"
if [ -z "$PATCH_USER" ]; then
	PATCH_USER="$(USER)"
fi
APP="adp" # APP前缀，用于后续通过rtools工具上传，如"KEP"
ORIGIN_SERVER="kb_config" # 二进制文件名，用于后续通过rtools工具上传，如"bot-task-config-server"
POLARIS_SERVANT="Admin" # 北极星服务名，用于后续通过rtools工具上传，如"task-config"

DUNGEON_SERVER="$ORIGIN_SERVER$1"
# # $DUNGEON 来自环境变量
# if [ -n "$DUNGEON" ]; then
#   DUNGEON_SERVER="$ORIGIN_SERVER-$DUNGEON$1"
# fi
echo "ORIGIN: ${APP}.${ORIGIN_SERVER}"
if [ "$ORIGIN_SERVER" != "$DUNGEON_SERVER" ]; then
  echo "DUNGEON: ${APP}.${DUNGEON_SERVER}"
fi

ENV="toe-base"
# $LANE 来自环境变量
if [ -n "$LANE" ]; then
  ENV="$LANE$1"
else
  echo "LANE 不存在, 请检查泳道环境变量"
  exit 0
fi
echo "ENV: $ENV"

LANG="trpc-golang"
TMP_DIR="${ORIGIN_SERVER}_tmp_dir"
TGZ="${ORIGIN_SERVER}.tgz"
BINARY_PATH="./cmd/server/kb-config"

# 确保 mac os 编译出来的包是 linux/amd64
export GOOS="linux"
export GOARCH="amd64"


# 编译构建
go build -ldflags "-X google.golang.org/protobuf/reflect/protoregistry.conflictPolicy=ignore" -o "$BINARY_PATH" ./cmd/server
gbr="$?"

if [ "$gbr" -ne 0 ]; then
  echo "编译失败, 请检查修复后重试~"
  exit
fi

# 检查编译产物是否存在
if [ ! -f "$BINARY_PATH" ]; then
  echo "错误：编译产物不存在: $BINARY_PATH"
  exit 1
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

# 修复：使用正确的二进制文件路径
cp -rf "$BINARY_PATH" "$TMP_DIR"/bin/"$DUNGEON_SERVER"
cd "$TMP_DIR" && tar -czvf ../"$TGZ" .
cd ../ && rm -rf "$TMP_DIR"

# 发布上传
"$RTOOLS" \
  -namespace Test \
  -env "$ENV" \
  -app "$APP" \
  -server "$DUNGEON_SERVER" \
  -package "$TGZ" \
  -user "$PATCH_USER" \
  -servant "$POLARIS_SERVANT" \
  -lang "$LANG"

if [ "$ORIGIN_SERVER" != "$DUNGEON_SERVER" ]; then
  "$sed0" -i "s/trpc0.DungeonForServer(\"$DUNGEON_SERVER\")/trpc0.DungeonForServer(\"$ORIGIN_SERVER\")/g" main.go
fi

rm -rf "$TGZ"
rm -rf "$BINARY_PATH"
rm -rf polaris