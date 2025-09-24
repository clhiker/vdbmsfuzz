# VDBMS 差分模糊测试框架

一个全面的差分模糊测试框架，通过原始HTTP API测试向量数据库管理系统，旨在识别不同VDBMS实现之间的不一致性、边缘情况和潜在安全问题。

## 支持的数据库

- **Milvus** (端口 19530) - 企业级向量数据库
- **Chroma** (端口 8000) - AI原生开源向量数据库
- **Qdrant** (端口 6333) - 高性能向量搜索引擎
- **Weaviate** (端口 8080) - 基于GraphQL的向量数据库

## 功能特点

### 核心测试能力

- **差分测试** - 在所有数据库上执行相同操作并比较结果
- **模糊测试生成** - 生成随机和边缘情况的测试向量和操作
- **多种操作类型** - 插入、搜索、删除、批处理操作和混合场景
- **边缘情况覆盖** - NaN/无穷大值、空向量、大维度、格式错误的ID
- **性能分析** - 执行时间跟踪和比较
- **优雅降级** - 即使某些数据库不可用也能继续测试

### 安全导向测试

- **防御性安全分析** - 识别可能导致安全问题的实现不一致性
- **输入验证测试** - 测试数据库如何处理格式错误和恶意输入
- **API稳定性测试** - 确保不同数据库版本的行为一致性
- **错误处理分析** - 比较错误响应和异常处理

## 快速开始

### 前置要求

- Python 3.8+
- 运行目标数据库的Docker容器（参见下面的Docker设置）
- 能够访问数据库端口的网络连接（19530, 8000, 6333, 8080）

### 安装

```bash
# 克隆仓库
git clone <repository-url>
cd vdbmsfuzz/fuzz

# 安装依赖
pip install -r requirements.txt
```

### 数据库设置

数据库应该在Docker容器中运行。Docker设置示例：

```bash
# Milvus
docker run -d --name milvus-standalone -p 19530:19530 milvusdb/milvus:latest

# Chroma
docker run -d --name chroma -p 8000:8000 chromadb/chroma:latest

# Qdrant
docker run -d --name qdrant -p 6333:6333 qdrant/qdrant:latest

# Weaviate
docker run -d --name weaviate -p 8080:8080 semitechnologies/weaviate:latest
```

### 运行测试

```bash
# 首先测试数据库连接
python test_connections.py

# 运行主模糊测试框架
python main.py

# 自定义测试数量
# 编辑 main.py 第104行：results = await fuzzer.run_fuzz_test(num_tests=50)
```

## 架构

### 核心组件

```
fuzz/
├── main.py                 # 主入口和测试协调器
├── db_clients.py          # 数据库客户端实现
├── differential_tester.py # 差分测试核心逻辑
├── fuzz_generator.py      # 模糊测试用例生成器
├── models.py              # 数据模型定义
├── config.py              # 配置管理
├── config.json            # 数据库配置文件
├── test_connections.py    # 连接测试工具
├── utils.py               # 工具类和结果分析
└── requirements.txt       # Python依赖
```

### 关键类

#### VDBMSFuzzer (main.py:29)
- 协调整个测试过程
- 处理数据库设置、测试执行和清理
- 支持模拟模式检测的优雅降级

#### 数据库客户端 (db_clients.py)
- `DatabaseClient` - 具有通用接口的抽象基类
- `MilvusClient` - 支持多版本的HTTP REST API客户端
- `ChromaClient` - ChromaDB的REST API客户端
- `QdrantClient` - 具有向后兼容性的HTTP API客户端
- `WeaviateClient` - 基于GraphQL的操作客户端

#### DifferentialTester (differential_tester.py:25)
- 跨所有数据库并发执行操作
- 使用数据库特定比较器比较结果
- 当数据库行为不同时记录不一致性

#### FuzzGenerator (fuzz_generator.py:22)
- 使用可配置概率生成随机测试用例
- 创建边缘情况：NaN、无穷大、空向量、格式错误的ID
- 支持多种操作类型和批处理场景

## 配置

### 数据库配置

框架使用`config.json`进行数据库设置。如果缺少，会自动生成：

```json
{
  "milvus": {
    "host": "localhost",
    "port": 19530,
    "database": "default",
    "collection": "test_collection"
  },
  "chroma": {
    "host": "localhost",
    "port": 8000,
    "collection": "test_collection"
  },
  "qdrant": {
    "host": "localhost",
    "port": 6333,
    "collection": "test_collection"
  },
  "weaviate": {
    "host": "localhost",
    "port": 8080,
    "collection": "TestCollection"
  },
  "test_settings": {
    "vector_dimension": 128,
    "num_collections": 5,
    "num_vectors_per_collection": 1000,
    "timeout_seconds": 30
  }
}
```

### 测试配置

修改`config.json`来调整：
- 向量维度和测试参数
- 数据库连接设置
- 超时值和集合名称
- 模糊测试概率和边缘情况生成

## 测试策略

### 测试流程

1. **设置阶段** - 建立数据库连接并创建测试集合
2. **测试生成** - 创建具有边缘情况的随机测试用例
3. **并发执行** - 在所有数据库上同时执行操作
4. **结果比较** - 比较响应并识别不一致性
5. **报告生成** - 生成包含统计信息的综合报告

### 支持的操作

- **插入操作** - 单个和批量向量插入，带元数据
- **搜索操作** - 向量相似性搜索，可配置度量（L2、余弦、内积）
- **删除操作** - 按ID删除向量
- **批处理操作** - 大规模向量插入和搜索
- **混合操作** - 不同操作类型的复杂序列

### 边缘情况

- 无效的向量维度（空、非常大）
- 特殊浮点值（NaN、无穷大）
- 格式错误的ID和集合名称
- 大批量操作
- 混合元数据类型
- 网络超时和连接失败

## 结果和分析

### 输出格式

测试结果保存在`results/`目录中，带时间戳：
```bash
results/
├── fuzz_results_20240921_143022.json
└── reports/
    ├── consistency_report.html
    └── performance_analysis.html
```

### 报告内容

每个测试结果包括：
- `test_id` - 唯一测试标识符
- `operation` - 执行的操作类型
- `inputs` - 使用的输入参数
- `results` - 数据库特定结果
- `inconsistencies` - 发现的不一致列表
- `execution_time` - 每个数据库的性能指标

### 分析指标

- **一致性率** - 具有一致结果的测试百分比
- **数据库成功率** - 单个数据库性能
- **操作统计** - 按操作类型的成功率
- **主要不一致性** - 发现的最常见问题

## 开发

### 项目结构

```python
# 核心数据模型
models.py:
    TestResult: 主测试结果数据类
    DatabaseResult: 单个数据库操作结果

# 配置管理
config.py:
    Config: 具有自动回退的配置管理器
    DatabaseConfig: 单个数据库设置

# 主框架
main.py:
    VDBMSFuzzer: 主协调器类
    入口点：asyncio.run(main())

# 数据库客户端
db_clients.py:
    DatabaseClient: 抽象基类
    MilvusClient, ChromaClient, QdrantClient, WeaviateClient

# 差分测试
differential_tester.py:
    DifferentialTester: 核心测试逻辑
    DatabaseResult: 单个数据库结果跟踪

# 测试生成
fuzz_generator.py:
    FuzzGenerator: 随机测试用例生成
    FuzzConfig: 可配置的模糊测试参数

# 工具类
utils.py:
    ResultAnalyzer: 报告生成和结果分析
    HealthChecker: 数据库健康监控
    ConfigValidator: 配置验证
```

### 添加新数据库

要添加对新向量数据库的支持：

1. **创建客户端类** - 在`db_clients.py`中扩展`DatabaseClient`
2. **实现必需方法** - `insert_vectors`、`search_vectors`、`delete_vectors`
3. **添加配置** - 更新`config.py`默认值和`config.json`
4. **更新健康检查** - 添加端点到`test_connections.py`
5. **添加结果比较器** - 在`differential_tester.py`中实现比较逻辑

### 示例：添加新数据库客户端

```python
class NewDatabaseClient(DatabaseClient):
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.base_url = f"http://{config.host}:{config.port}"

    async def connect(self):
        # 实现连接逻辑
        pass

    async def insert_vectors(self, vectors: List[List[float]], ids: List[str], metadata: List[Dict] = None):
        # 实现向量插入
        pass

    async def search_vectors(self, query_vector: List[float], k: int, metric: str = "L2"):
        # 实现向量搜索
        pass

    async def delete_vectors(self, ids: List[str]):
        # 实现向量删除
        pass
```

## 安全考虑

### 防御性安全方法

本框架专为**防御性安全测试**设计：
- ✅ 识别数据库实现之间的不一致性
- ✅ 测试边缘情况和无效输入
- ✅ 确保跨版本API稳定性
- ✅ 通过差分分析发现潜在安全问题
- ❌ 不具备攻击性安全或利用能力

### 输入验证测试

框架测试各种输入验证场景：
- 格式错误的向量数据和维度
- 无效的ID格式和集合名称
- 大批量操作和超时场景
- 网络分区和连接失败

### API稳定性测试

- 不同数据库版本的一致行为
- 标准化的错误响应和异常处理
- 负载下的性能一致性
- 资源管理和清理

## 贡献

1. Fork仓库
2. 创建功能分支
3. 添加新数据库支持或测试场景
4. 在所有支持的数据库上彻底测试
5. 提交包含详细描述的拉取请求

## 许可证

本项目用于防御性安全研究和教育目的。

## 联系方式

有关问题、疑问或贡献，请在仓库中开issue。