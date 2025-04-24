# 订单数据处理系统

## 项目概述

该系统用于处理不同销售渠道（支付宝、企业微信、天猫、淘宝）的订单数据，将不同格式的订单数据提取、转换并加载到统一的数据库中，同时支持生成标准化的Excel报表。系统采用RESTful API接口设计，支持批量上传和处理订单文件。

## 功能特点

- 支持多渠道订单数据处理（支付宝、企业微信、天猫、淘宝）
- 智能识别与匹配不同格式Excel文件中的列名
- 自动处理退款数据并更新订单状态
- 统一的数据输出格式，便于后续分析和管理
- RESTful API接口，支持外部系统集成
- 灵活的错误处理和日志记录
- 支持并发处理多个渠道的订单数据

## 系统架构

- **前端**：基于HTTP的文件上传表单（可扩展为Web界面）
- **后端**：Flask框架的Python应用
- **数据库**：MySQL存储处理后的订单数据
- **文件存储**：临时存储上传的Excel文件和生成的处理结果文件
- **处理引擎**：针对不同渠道的专用数据处理模块

## API 接口

### 处理订单数据

- **URL**: `/process_order`
- **方法**: POST
- **Content-Type**: multipart/form-data
- **参数**: 
  - `channel`: 渠道名称（支付宝、企业微信、天猫、淘宝）
  - `order_file`: 订单文件（Excel格式）
  - `refund_order_file`: 退款文件（Excel格式，仅天猫和淘宝渠道需要）
- **响应**: JSON格式，包含处理状态和结果文件信息

## 渠道处理逻辑

### 1. 支付宝渠道

- **输入**：单个订单Excel文件
- **处理流程**：
  - 跳过表头和表尾，提取有效交易记录
  - 映射字段：商户订单号→订单编号，支付宝交易号→支付单号等
  - 计算手续费：服务费-退服务费
  - 处理空值和数据类型转换
- **特点**：直接从源文件提取数据，无需复杂合并操作

### 2. 企业微信渠道

- **输入**：单个Excel文件（sheet名必须为"result"）
- **处理流程**：
  - 过滤掉"提现"类型的记录
  - 按"关联单号"分组处理数据
  - 分类累加不同类型的记录：收款、退款、手续费
  - 汇总每个订单的交易情况
- **特点**：需要聚合多条相关记录，处理不同动账类型

### 3. 天猫/淘宝渠道

- **输入**：两个Excel文件（订单文件和退款文件）
- **处理流程**：
  - 验证文件命名（订单文件不含"退款"，退款文件必须含"退款"）
  - 智能匹配列名，适应不同格式的Excel文件
  - 处理订单数据：提取、标准化字段
  - 处理退款数据：更新订单状态，计算实际金额
  - 处理跨月份退款：创建新的订单记录
  - 合并所有数据，生成最终结果
- **特点**：需要处理两个文件，存在复杂的数据合并和状态更新逻辑

## 数据库结构

系统使用`orders`表存储所有处理后的订单数据，主要字段包括：

- `order_id`: 订单编号（主键）
- `payment_id`: 支付单号
- `amount`: 买家实付金额
- `status`: 订单状态
- `create_time`: 订单创建时间
- `merchant_remark`: 商家备注
- `refund_amount`: 卖家实退金额
- `fee`: 手续费
- `channel`: 渠道（支付宝/企业微信/天猫/淘宝）
- `confirm_time`: 确认收货时间
- `merchant_payment`: 打款商家金额
- `created_at`: 记录创建时间
- `updated_at`: 记录更新时间
- `customer_id`: 订单对应客服
- `writer_id`: 订单对应写手

## 安装与配置

### 环境要求

- Python 3.7+
- MySQL 8.0+
- 必要的Python库：Flask, pandas, mysql-connector-python, xlrd, openpyxl

### 安装步骤

1. 克隆代码库
   ```
   git clone [仓库地址]
   cd order-filter
   ```

2. 安装依赖
   ```
   pip install -r requirements.txt
   ```

3. 配置数据库连接
   编辑`process.py`文件中的`DB_CONFIG`配置：
   ```python
   DB_CONFIG = {
       'host': '数据库服务器地址',
       'user': '用户名',
       'password': '密码',
       'database': '数据库名',
       'port': 3306
   }
   ```

4. 创建上传目录
   ```
   mkdir uploads
   ```

5. 启动服务
   ```
   python process.py
   ```

## 使用示例

### 支付宝渠道
```
curl -X POST http://localhost:5000/process_order \
  -F "channel=支付宝" \
  -F "order_file=@/path/to/alipay_order.xlsx"
```

### 企业微信渠道
```
curl -X POST http://localhost:5000/process_order \
  -F "channel=企业微信" \
  -F "order_file=@/path/to/wechat_order.xlsx"
```

### 天猫/淘宝渠道
```
curl -X POST http://localhost:5000/process_order \
  -F "channel=淘宝" \
  -F "order_file=@/path/to/taobao_order.xlsx" \
  -F "refund_order_file=@/path/to/taobao_refund.xlsx"
```

## 注意事项

1. 所有上传的文件必须是Excel格式（.xlsx）
2. 企业微信渠道的Excel文件必须包含名为"result"的sheet
3. 天猫和淘宝渠道必须同时上传订单文件和退款文件
4. 处理后的数据会保存到数据库，并生成新的Excel文件
5. 数据库使用订单编号（order_id）作为主键，重复数据会更新而不是插入
6. 系统具有列名智能匹配功能，能适应不同格式的Excel文件

## 异常处理

系统会返回统一格式的错误信息：
```json
{
    "code": 1,
    "message": "错误信息描述",
    "data": null
}
```

常见错误包括：
- 参数错误（未指定渠道、没有上传文件等）
- 文件格式错误（不是有效的Excel文件）
- 文件命名不符合规则
- 数据处理错误
- 数据库操作错误

## 运行环境

- 操作系统：Windows/Linux/macOS
- 支持的浏览器：Chrome, Firefox, Safari, Edge
- 内存要求：至少2GB RAM
- 磁盘空间：至少500MB可用空间 