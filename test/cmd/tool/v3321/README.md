# KB-Config V3321 工具使用说明

本工具提供了一系列命令行工具，用于管理和维护知识库配置中的应用（App）和问答（QA）资源。

## 目录

- [应用管理命令 (app)](#应用管理命令-app)
  - [列出应用 (app list)](#列出应用-app-list)
  - [更新应用容量 (app update-used-capacity)](#更新应用容量-app-update-used-capacity)
- [问答管理命令 (qa)](#问答管理命令-qa)
  - [列出问答 (qa list)](#列出问答-qa-list)
  - [更新问答大小 (qa update-qa-size)](#更新问答大小-qa-update-qa-size)

---

## 应用管理命令 (app)

### 列出应用 (app list)

列出符合条件的应用列表。

#### 使用方法

```bash
./kb-config-v3321 app list [flags]
```

#### 参数说明

- `--corp_id`: 企业ID（可选）
- `--space_id`: 空间ID（可选）
- `--page_size`: 每页显示数量（可选，默认值根据配置）

#### 示例

```bash
# 列出指定企业的所有应用
./kb-config-v3321 app list --corp_id=123456

# 列出指定空间的所有应用
./kb-config-v3321 app list --space_id="space_abc123"

# 列出应用并限制每页数量
./kb-config-v3321 app list --corp_id=123456 --page_size=50
```

#### 输出字段

- `corp_id`: 企业ID
- `space_id`: 空间ID
- `id`: 应用主键ID
- `biz_id`: 应用业务ID
- `name`: 应用名称
- `uin`: 用户UIN
- `is_shared`: 是否共享
- `is_exp_center`: 是否为实验中心
- `qa_version`: 问答版本
- `used_char_size`: 已使用字符大小

---

### 更新应用容量 (app update-used-capacity)

批量更新应用的容量使用情况，包括知识容量、存储容量和计算容量。

#### 功能说明

该命令会：
1. 计算应用下所有未删除文档的 `file_size` 总和
2. 计算应用下所有未删除问答的 `qa_size` 总和
3. 更新 `t_robot` 表的三个字段：
   - `used_knowledge_capacity` = 文档file_size总和 + 问答qa_size总和
   - `used_storage_capacity` = 非SourceFromCorpCOSDoc文件的file_size总和
   - `used_compute_capacity` = used_knowledge_capacity

#### 使用方法

```bash
./kb-config-v3321 app update-used-capacity --config=<配置文件路径>
```

#### 参数说明

- `--config`: YAML配置文件路径（必需）

#### 配置文件格式

配置文件使用YAML格式，支持两种模式：

##### 模式1：处理所有企业

```yaml
all_uin: true
```

##### 模式2：处理指定企业

```yaml
all_uin: false
uin_list:
  - uin: 123456
    all: true  # 处理该企业下所有应用
  
  - uin: 789012
    space_id: "space_abc123"  # 处理指定空间下的所有应用
  
  - uin: 345678
    app_biz_ids:  # 处理指定的应用
      - 1001
      - 1002
      - 1003
```

#### 配置参数说明

- `all_uin`: 是否处理所有企业（true/false）
- `uin_list`: 企业列表（当all_uin为false时使用）
  - `uin`: 企业UIN（必需）
  - `all`: 是否处理该企业下所有应用（可选）
  - `space_id`: 指定空间ID（可选，与app_biz_ids互斥）
  - `app_biz_ids`: 指定应用业务ID列表（可选，与space_id互斥）

#### 参数组合规则

- 当 `all=true` 时，`space_id` 和 `app_biz_ids` 必须为空
- 当 `all=false` 时，`space_id` 和 `app_biz_ids` 只能指定一个
- 每个uin必须至少指定 `all`、`space_id` 或 `app_biz_ids` 之一

#### 示例

```bash
# 处理所有企业的所有应用
./kb-config-v3321 app update-used-capacity --config=config_all.yaml

# 处理指定企业的应用
./kb-config-v3321 app update-used-capacity --config=config_specific.yaml
```

#### 特性说明

1. **Redis缓存机制**：已成功处理的应用会记录到Redis中，有效期30天，避免重复处理
2. **分批处理**：自动分批查询文档和问答，避免内存溢出
3. **升级过程锁定知识库**：处理过程中会调用embedding版本升级接口锁定知识库
4. **错误处理**：Redis错误不影响主流程，会记录日志但继续处理

---

## 问答管理命令 (qa)

### 列出问答 (qa list)

列出符合条件的问答资源。

#### 使用方法

```bash
./kb-config-v3321 qa list [flags]
```

#### 参数说明

- `--biz_ids`: 问答业务ID列表，多个ID用逗号分隔（可选）
- `--corp_id`: 企业ID（可选）
- `--fields`: 要显示的数据库字段名，多个字段用逗号分隔（可选）

#### 示例

```bash
# 列出指定业务ID的问答
./kb-config-v3321 qa list --biz_ids=1001,1002,1003

# 列出指定企业的问答，并显示特定字段
./kb-config-v3321 qa list --corp_id=123456 --fields=id,business_id,question,answer,qa_size

# 列出问答并显示所有默认字段
./kb-config-v3321 qa list --corp_id=123456
```

#### 默认显示字段

- `id`: 问答ID
- `business_id`: 业务ID
- `corp_id`: 企业ID
- `robot_id`: 应用ID
- `question`: 问题内容

---

### 更新问答大小 (qa update-qa-size)

更新问答资源的 `qa_size` 字段，包括问答本身和相似问。

#### 功能说明

该命令会：
1. 计算每个问答的问题和答案长度
2. 计算该问答下所有相似问的长度
3. 更新问答的 `qa_size` 字段为总长度
4. 更新每个相似问的 `qa_size` 字段

#### 使用方法

```bash
./kb-config-v3321 qa update-qa-size [flags]
```

#### 参数说明

- `--type` 或 `-t`: 更新类型，可选值：field 或 label（可选，默认为 field）
- `--uin`: 企业UIN（必需）
- `--app_biz_ids`: 应用业务ID列表，多个ID用逗号分隔（可选）
- `--space_id`: 空间ID（可选）
- `--all`: 处理该UIN下所有应用（可选）

#### 参数组合规则

- `--app_biz_ids`、`--space_id` 和 `--all` 三个参数互斥，只能指定一个
- 必须至少指定 `--app_biz_ids`、`--space_id` 或 `--all` 之一

#### 示例

```bash
# 更新指定企业下所有应用的问答大小
./kb-config-v3321 qa update-qa-size --uin=123456 --all

# 更新指定空间下所有应用的问答大小
./kb-config-v3321 qa update-qa-size --uin=123456 --space_id="space_abc123"

# 更新指定应用的问答大小
./kb-config-v3321 qa update-qa-size --uin=123456 --app_biz_ids=1001,1002,1003

# 指定更新类型
./kb-config-v3321 qa update-qa-size --type=label --uin=123456 --all
```

#### 处理流程

1. 分批查询应用下所有未删除的问答
2. 对每个问答：
   - 计算问题和答案的长度
   - 分批查询该问答的所有相似问
   - 计算每个相似问的长度并更新
   - 累加总长度并更新问答的 `qa_size`

---

## 通用参数

以下参数适用于所有命令：

- `--help` 或 `-h`: 显示帮助信息

## 配置初始化

所有命令在执行前都会自动初始化配置，包括：
- 数据库连接
- Redis连接
- RPC服务连接

## 日志说明

工具会输出详细的日志信息，包括：
- 处理进度
- 成功/失败状态
- 错误详情
- 统计信息

## 注意事项

1. **权限要求**：确保有足够的权限访问数据库和Redis
2. **配置文件**：配置文件路径必须正确且格式符合YAML规范
3. **参数验证**：工具会在执行前验证参数的合法性和组合逻辑
4. **批处理**：大量数据会自动分批处理，避免内存问题
5. **幂等性**：`app update-used-capacity` 命令通过Redis缓存实现幂等性，避免重复处理

## 错误处理

- 参数错误：会立即返回错误信息并退出
- 数据库错误：会记录详细日志并返回错误
- Redis错误：不影响主流程，会记录日志但继续执行
- RPC错误：会记录详细日志并返回错误

## 示例配置文件

### config_all.yaml
```yaml
# 处理所有企业
all_uin: true
```

### config_specific.yaml
```yaml
# 处理指定企业
all_uin: false
uin_list:
  # 处理企业123456下的所有应用
  - uin: 123456
    all: true
  
  # 处理企业789012下指定空间的应用
  - uin: 789012
    space_id: "space_abc123"
  
  # 处理企业345678下指定的应用
  - uin: 345678
    app_biz_ids:
      - 1001
      - 1002
      - 1003
```

## 技术支持

如有问题，请联系开发团队或查看源代码注释。
