#!/bin/bash

#####################################################################
#                                                                   #
#  @(#)git-tag.sh  March 27, 2024                                   #
#  Copyright(c) 2024, halelv@Tencent. All rights reserved.          #
#                                                                   #
#####################################################################

# 从 master 生成最新的 TAG

# 进入到脚本的目录
cd "$(dirname "${BASH_SOURCE[0]}")"

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

if [ "$OS_DARWIN" == "$WORKING_OS" ]; then
  sed0="gsed"
fi

#######################################
# 从 master 最新的提交创建新的 TAG
# Globals:
#   None
# Arguments:
#   [1] <string> 分支名字, TAG 来源
# Outputs:
#   STDOUT
# Returns:
#   None
#######################################
function create_tag() {
  current_branch="$(git branch | awk '/\*/ { print $2; }')"
  echo "当前分支: $current_branch => 目标分支: $1"
  # 更新最新代码
  git stash
  git checkout "$1"
  git pull

  echo "---------------------------------------------------"
  git tag -l | xargs git tag -d > /dev/null
  git fetch --tags -q
  local last_tag="$(git for-each-ref --format='%(*committerdate:raw)%(committerdate:raw) %(refname:short) %(objectname:short)' 'refs/tags' | sort -nr | head -n 1 | awk '{print $3}')"
  echo "Last Tag: $last_tag"
  # 检查 $1 的 hash
  target_hash=$(git log -n 200 --pretty='%h %D' | grep "origin/$1\(,\|)\|\$\)" | awk '{print $1}' | head -n 1)
  if [ -z "$target_hash" ]; then
    echo "请将近期提交的代码合并到 $1 再打 TAG."
    git checkout "$current_branch"
    git stash pop
    return
  fi
  echo "基线 SHA1 => [$target_hash]"
  # server name
  server=$(grep '^module' ../go.mod | awk -F '/' '{print $NF}')
  if [ "$1" != "master" ]; then
    server=$(echo "${server}.$1" | "$sed0" 's|/|.|g')
  fi
  # date
  date0=$(date '+%Y%m%d')
  # build no
  count=$(($(git tag | wc -l) + 1))
  build_no="$(printf '%04d' $count)"
  # full name
  tag_name=$(echo "TAG-${server}.${date0}.build.${build_no}" | "$sed0" 's/_/-/g')
  echo "New Tag: $tag_name"
  echo "---------------------------------------------------"

  git tag "$tag_name" "$target_hash"
  git push origin "$tag_name"

  git checkout "$current_branch"
  git stash pop
  echo "---------------------------------------------------"
  echo "OK"
  echo "==================================================="
}



#######################################
# bash 执行入口函数
# Globals:
#   None
# Arguments:
#   [1] <string> 是否使用当前分支来打 TAG, 当为字符串 'true' 时, 使用当前分支打 TAG
# Outputs:
#   STDOUT
# Returns:
#   None
#######################################
function main() {
echo $1
  if [ "$1" == "true" ]; then
    create_tag "$(git branch | awk '/\*/ { print $2; }')"
  else
    create_tag 'master'
  fi
}

#
main "$1"