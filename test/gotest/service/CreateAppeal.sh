#!/bin/bash

cd `dirname "${BASH_SOURCE[0]}"`

# 引用公共函数和变量
source ../server-env.sh

$TRPC_CLI -datafiles=CreateAppeal.json -target="$TARGET" -out=CreateAppeal.out

