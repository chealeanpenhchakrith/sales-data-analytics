# Sales Data Analytics with Apache Spark 实现步骤

## 1. 项目总结

本项目要求基于 UCI Online Retail Dataset 构建一个销售数据分析应用。数据集包含约 54 万条英国线上零售商交易记录，字段包括 `InvoiceNo`、`StockCode`、`Description`、`Quantity`、`InvoiceDate`、`UnitPrice`、`CustomerID`、`Country`。

核心目标是使用 Apache Spark，尤其是 PySpark，高效处理历史销售数据，帮助电商公司理解业务表现、分析客户行为，并识别提升商业表现的机会。

项目最低要求包括：

- 使用 Docker 环境运行 Apache Spark。
- 使用 PySpark 实现数据分析应用。
- 加载数据集。
- 对数据进行必要的清洗与转换。
- 围绕销售、产品、客户、国家等维度进行多项相关分析。
- 向用户展示分析结果。
- 计算有意义的业务指标和关键绩效指标。
- 将数据集文件存储到 Hadoop HDFS。
- 最终提交 PDF 报告，并进行不超过 10 分钟的口头展示和现场演示。

可选扩展：

- 增加实时数据处理或流式处理方案。
- 派生更多业务字段。
- 丰富分析结果或可视化展示。

## 2. 当前仓库状态

当前项目已有文件：

- `Project – Sales Data Analytics with Apache Spark-1.pdf`：项目说明。
- `src/data/Online Retail.csv`：原始销售数据，使用分号 `;` 分隔，价格字段使用逗号作为小数分隔符。
- `src/script.py`：现有 PySpark 脚本，目前只完成 CSV 读取和 `show()` 展示。
- `README.md`：项目标题。

现有脚本需要继续扩展为完整的数据清洗、指标计算和分析输出程序。

## 3. 推荐实现步骤

### Step 1：准备运行环境

1. 确认本机安装 Docker 和 Docker Compose。
   - 验证命令：
     ```bash
     docker --version
     docker compose version
     ```
2. 准备包含 Spark、PySpark、Hadoop/HDFS 的运行环境，建议统一使用 Docker Compose 管理，避免手动安装 Spark 和 Hadoop。
3. 建议新增 `docker-compose.yml`，至少包含以下服务：
   - Spark master。
   - Spark worker。
   - HDFS NameNode。
   - HDFS DataNode。
4. 建议暴露并记录以下端口：
   - Spark Master UI：`http://localhost:8080`
   - Spark Master service：`spark://spark-master:7077`
   - HDFS NameNode Web UI：`http://localhost:9870`
   - HDFS RPC 地址：`hdfs://namenode:9000`
5. 建议在 `docker-compose.yml` 中配置数据和代码挂载：
   - 将项目目录挂载到 Spark 容器，例如 `/app`。
   - 将 `src/data/` 挂载到容器内，便于上传 CSV 到 HDFS。
   - 为 NameNode 和 DataNode 配置 Docker volume，避免容器重启后 HDFS 数据丢失。
6. 启动环境：
   ```bash
   docker compose up -d
   ```
7. 检查容器状态：
   ```bash
   docker compose ps
   ```
   所有 Spark 和 HDFS 相关容器应处于 `running` 或 `Up` 状态。
8. 验证 Spark 是否可用：
   - 打开 `http://localhost:8080`，确认可以看到 Spark Master 页面。
   - 页面中应能看到至少一个 worker 已连接。
9. 验证 HDFS 是否可用：
   - 打开 `http://localhost:9870`，确认可以看到 NameNode 页面。
   - 在容器中执行：
     ```bash
     hdfs dfs -ls /
     ```
     如果能正常返回目录列表，说明 HDFS 基础服务可用。
10. 准备 PySpark 作业运行入口，建议使用 `spark-submit` 执行项目脚本，例如：
    ```bash
    docker compose exec spark-master spark-submit \
      --master spark://spark-master:7077 \
      /app/src/script.py
    ```
11. Step 1 的完成标准：
    - `docker compose up -d` 可以成功启动全部服务。
    - Spark UI 可以访问，并显示 worker。
    - HDFS Web UI 可以访问。
    - `hdfs dfs -ls /` 可以执行成功。
    - `spark-submit` 可以运行 PySpark 脚本。

### Step 2：加载数据

1. 使用 PySpark 读取 `src/data/Online Retail.csv`。
2. 设置正确读取参数：
   - `header=True`
   - `sep=";"`
   - 保留 UTF-8 BOM 兼容处理。
3. 检查 schema、总行数、空值数量和样例数据。
4. 后续 HDFS 部分完成后，将读取路径切换或扩展为 HDFS 路径，例如 `hdfs://namenode:9000/data/Online Retail.csv`。

### Step 3：数据清洗

1. 清理字段名，例如去除 BOM 导致的异常列名。
2. 转换字段类型：
   - `Quantity` 转为整数。
   - `UnitPrice` 将逗号小数替换为点号后转为 double。
   - `InvoiceDate` 按 `dd/MM/yyyy HH:mm` 转为 timestamp。
   - `CustomerID` 保留为字符串或转为整数，视空值处理策略决定。
3. 处理无效记录：
   - 删除 `Description` 为空的记录。
   - 删除或单独标记 `CustomerID` 为空的记录。
   - 过滤 `Quantity <= 0` 或 `UnitPrice <= 0` 的异常销售记录。
   - 对 `InvoiceNo` 以 `C` 开头的取消订单单独统计或从常规销售分析中排除。
4. 新增派生字段：
   - `Revenue = Quantity * UnitPrice`
   - `InvoiceDateOnly`
   - `Year`
   - `Month`
   - `YearMonth`
   - `DayOfWeek`
   - `Hour`

### Step 4：计算核心 KPI

建议至少实现以下指标：

1. 总销售额：`sum(Revenue)`。
2. 总订单数：`countDistinct(InvoiceNo)`。
3. 总销量：`sum(Quantity)`。
4. 客户数：`countDistinct(CustomerID)`。
5. 商品数：`countDistinct(StockCode)`。
6. 国家数：`countDistinct(Country)`。
7. 平均订单金额：`sum(Revenue) / countDistinct(InvoiceNo)`。
8. 平均客单价：`sum(Revenue) / countDistinct(CustomerID)`。
9. 取消订单数量与占比。

### Step 5：销售分析

1. 按月统计销售额、订单数、销量。
2. 按星期统计销售额，观察一周内销售规律。
3. 按小时统计销售额或订单量，观察高峰时段。
4. 找出销售额最高和最低的月份。
5. 输出销售趋势表，为报告中的趋势分析提供依据。

### Step 6：产品分析

1. 按 `StockCode` 和 `Description` 聚合：
   - 商品总销量。
   - 商品总销售额。
   - 商品订单次数。
2. 输出 Top 10 畅销商品。
3. 输出 Top 10 收入最高商品。
4. 识别低销量或低收入商品，用于库存和商品策略分析。

### Step 7：客户分析

1. 按 `CustomerID` 聚合：
   - 客户总消费额。
   - 客户订单数。
   - 客户购买件数。
   - 最近一次购买日期。
2. 输出 Top 10 高价值客户。
3. 可选实现 RFM 分析：
   - Recency：距离最后一次购买的天数。
   - Frequency：订单频次。
   - Monetary：消费金额。
4. 基于 RFM 或消费金额对客户进行分层，例如高价值客户、活跃客户、普通客户、流失风险客户。

### Step 8：国家分析

1. 按 `Country` 聚合：
   - 销售额。
   - 订单数。
   - 客户数。
   - 销量。
2. 输出 Top 10 销售额最高国家。
3. 对比英国和非英国市场占比。
4. 找出潜在增长市场。

### Step 9：HDFS 集成

1. 启动 Hadoop HDFS 容器。
2. 在 HDFS 中创建数据目录，例如：
   - `/data/online-retail/`
3. 将本地 CSV 上传到 HDFS：
   - `hdfs dfs -mkdir -p /data/online-retail`
   - `hdfs dfs -put src/data/Online\ Retail.csv /data/online-retail/`
4. 修改 PySpark 脚本支持从 HDFS 读取。
5. 运行分析程序，确认 HDFS 路径可读。

### Step 10：结果输出

1. 在终端打印关键 KPI 和 Top N 分析结果。
2. 可选将分析结果写入 `output/` 目录：
   - CSV。
   - Parquet。
   - JSON。
3. 为最终报告保留截图或导出结果表。
4. 输出应保持清晰，例如分区展示：
   - Overall KPI
   - Sales Trend
   - Top Products
   - Top Customers
   - Country Performance

### Step 11：可选流式处理扩展

如果时间允许，可以增加一个简化版 streaming demo：

1. 模拟实时订单数据输入。
2. 使用 Spark Structured Streaming 读取文件流或 socket 流。
3. 实时计算最近窗口内的销售额、订单数或热门商品。
4. 在最终展示中说明这是对真实电商实时销售监控的扩展方案。

### Step 12：最终报告与展示

报告建议结构：

1. 项目背景与业务问题。
2. 数据集介绍。
3. 技术架构：Docker、Spark、PySpark、HDFS。
4. 数据清洗流程。
5. KPI 指标定义。
6. 销售、产品、客户、国家分析结果。
7. 商业洞察与建议。
8. 可选扩展：流式处理设计。
9. 遇到的问题与解决方案。
10. 结论。

10 分钟展示建议安排：

1. 1 分钟：项目目标和数据集。
2. 2 分钟：架构与数据处理流程。
3. 3 分钟：核心分析结果和业务洞察。
4. 2 分钟：现场运行 PySpark 分析程序。
5. 1 分钟：HDFS 或 streaming 扩展说明。
6. 1 分钟：总结与问答准备。

## 4. 建议代码结构

建议将当前单文件脚本逐步整理为：

```text
src/
  script.py
  data/
    Online Retail.csv
output/
docker-compose.yml
README.md
agents.md
```

如果功能继续增长，可以进一步拆分：

```text
src/
  main.py
  config.py
  data_loader.py
  cleaning.py
  analytics.py
  reporting.py
```

短期内为了课程项目演示，保留单个 `src/script.py` 也可以接受，但应保证函数结构清晰，避免所有逻辑直接写在全局作用域中。

## 5. 实现优先级

优先级从高到低：

1. 完成 PySpark 正确读取 CSV。
2. 完成字段类型转换和数据清洗。
3. 完成核心 KPI。
4. 完成销售、产品、客户、国家四类分析。
5. 将结果清晰打印或导出。
6. 加入 Docker 运行方式。
7. 加入 HDFS 上传和读取。
8. 准备报告和演示脚本。
9. 如有余力，再做 streaming 扩展。
