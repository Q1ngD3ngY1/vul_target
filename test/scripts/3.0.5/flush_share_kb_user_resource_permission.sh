#!/bin/bash

# 需要处理的t_share_knowledge表的id范围
START_ID=1
END_ID=100000

# 发送 POST 请求
curl -X POST http://21.21.124.140:8081/kb/FlushShareKbUserResourcePermission \
  -H "Content-Type: application/json" \
  -d '{
    "start_id": '$START_ID',
    "end_id": '$END_ID'
  }'