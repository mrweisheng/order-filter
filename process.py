import pandas as pd
import os
from flask import Flask, request, jsonify
import werkzeug.utils
import mysql.connector
from mysql.connector import Error
import uuid
import time
from flask_cors import CORS
import threading
import queue

app = Flask(__name__)
CORS(app)

# 添加处理状态跟踪
processing_tasks = {}
task_queue = queue.Queue()

# 设置上传文件的保存目录
UPLOAD_FOLDER = 'uploads'
if not os.path.exists(UPLOAD_FOLDER):
    os.makedirs(UPLOAD_FOLDER)

# 数据库配置
DB_CONFIG = {
    'host': '118.31.76.202',  # 修改为服务器的真实IP地址
    'user': 'root',
    'password': '1qaz!QAZ2wsx@WSX',
    'database': 'wenzhi',
    'port': 3306,  # 添加明确的端口号
    'charset': 'utf8mb4',  # 添加明确的字符集
    'autocommit': True,  # 尝试添加自动提交
    'use_pure': True  # 使用纯Python实现以避免C扩展问题
}

def process_alipay(file_path):
    """处理支付宝订单数据"""
    try:
        print(f"开始读取支付宝文件: {file_path}")
        
        # 检查文件扩展名，如果是CSV文件直接使用CSV读取
        if file_path.lower().endswith('.csv'):
            print("检测到CSV文件，使用CSV读取引擎...")
            
            # 尝试多种编码读取CSV文件
            encodings = ['utf-8', 'gbk', 'gb2312', 'gb18030', 'latin1']
            df = None
            
            for encoding in encodings:
                try:
                    print(f"尝试使用 {encoding} 编码读取...")
                    df = pd.read_csv(file_path, skiprows=2, encoding=encoding)
                    print(f"使用 {encoding} 编码读取成功，原始行数: {len(df)}")
                    break
                except UnicodeDecodeError as e:
                    print(f"{encoding} 编码读取失败: {str(e)}")
                    continue
                except Exception as e:
                    print(f"{encoding} 编码读取失败: {str(e)}")
                    continue
            
            if df is None:
                raise ValueError("无法读取CSV文件，尝试了多种编码都失败")
        else:
            # Excel文件的多引擎尝试
            # 尝试使用xlrd引擎读取（支持.xls和.xlsx格式，兼容性最好）
            try:
                print("尝试使用xlrd引擎读取...")
                df = pd.read_excel(file_path, skiprows=2, engine='xlrd')
                print(f"使用xlrd引擎读取成功，原始行数: {len(df)}")
            except Exception as e1:
                print(f"xlrd引擎读取失败: {str(e1)}")
                
                # 尝试使用openpyxl引擎读取（只支持.xlsx格式）
                try:
                    print("尝试使用openpyxl引擎读取...")
                    df = pd.read_excel(file_path, skiprows=2, engine='openpyxl')
                    print(f"使用openpyxl引擎读取成功，原始行数: {len(df)}")
                except Exception as e2:
                    print(f"openpyxl引擎读取失败: {str(e2)}")
                    
                    # 尝试不指定引擎（pandas自动选择）
                    try:
                        print("尝试使用pandas默认引擎读取...")
                        df = pd.read_excel(file_path, skiprows=2)
                        print(f"使用pandas默认引擎读取成功，原始行数: {len(df)}")
                    except Exception as e3:
                        print(f"pandas默认引擎读取失败: {str(e3)}")
                        
                        # 尝试读取HTML表格（可能是网页导出的数据）
                        try:
                            print("尝试读取HTML表格...")
                            # 读取所有HTML表格
                            html_tables = pd.read_html(file_path)
                            if html_tables:
                                df = html_tables[0]  # 使用第一个表格
                                print(f"使用HTML表格读取成功，原始行数: {len(df)}")
                            else:
                                raise ValueError("HTML文件中没有找到表格")
                        except Exception as e4:
                            print(f"HTML表格读取失败: {str(e4)}")
                            
                            print(f"所有格式都读取失败:")
                            print(f"  xlrd错误: {str(e1)}")
                            print(f"  openpyxl错误: {str(e2)}")
                            print(f"  pandas默认引擎错误: {str(e3)}")
                            print(f"  HTML表格错误: {str(e4)}")
                            raise ValueError(f"无法读取文件，请检查文件格式是否正确。支持格式：.xlsx, .xls, .csv, HTML表格。错误信息: {str(e1)}")
        
        # 验证数据是否为空
        if df.empty:
            raise ValueError("文件中没有数据")
        
        print(f"文件列名: {df.columns.tolist()}")
        
        # 验证必要字段是否存在
        required_fields = ['商户订单号', '支付宝交易号', '商家实收(元)', '交易状态', '创建时间']
        missing_fields = []
        for field in required_fields:
            if field not in df.columns:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"文件缺少必要字段: {missing_fields}")
        
        # 移除最后一行（通常是汇总行）
        if len(df) > 0:
            df = df[:-1]
            print(f"移除最后一行后，数据行数: {len(df)}")
        
        # 验证数据行数
        if len(df) == 0:
            raise ValueError("处理后没有有效数据行")

        # 创建新的DataFrame，只包含需要的列，并按规则处理数据
        new_df = pd.DataFrame({
            '订单编号': df['商户订单号'],
            '支付单号': df['支付宝交易号'],
            '买家实付': df['商家实收(元)'],
            '订单状态': df['交易状态'],
            '订单创建时间': df['创建时间'],
            '商家备注': df['付款备注'].fillna(''),
            '卖家实退': df['商家实退(元)'].fillna(0),
            '手续费': df.apply(lambda row: (row['服务费(元)'] - row['退服务费(元)']) if pd.notna(row['服务费(元)']) and pd.notna(row['退服务费(元)']) else row['服务费(元)'], axis=1),
            '渠道': '支付宝',
            '确认收货时间': df.get('确认收货时间', ''),
            '打款商家金额': df.get('打款商家金额', '')
        })

        print(f"支付宝数据处理完成，结果行数: {len(new_df)}")
        return new_df
        
    except Exception as e:
        print(f"处理支付宝数据时出错: {str(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        raise

def process_wechat(df):
    """处理微信订单数据"""
    print(f"开始处理企业微信数据，原始数据行数: {len(df)}")
    print(f"原始数据列名: {df.columns.tolist()}")
    
    output_data = []
    
    # 过滤掉动账类型为提现的记录
    df = df[df['动账类型'] != '提现']
    print(f"过滤掉提现记录后，剩余行数: {len(df)}")
    
    # 按"关联单号"分组
    grouped = df.groupby("关联单号")
    print(f"分组后的关联单号数量: {len(grouped)}")

    for order_id, group in grouped:
        print(f"处理关联单号: {order_id}, 该组数据行数: {len(group)}")
        
        # 安全的字段访问 - 添加字段存在性检查
        payment_id = group["商户单号"].iloc[0] if "商户单号" in group.columns else ""
        remark = group["备注"].iloc[0] if "备注" in group.columns and len(group) > 0 else ""
        
        order_data = {
            "订单编号": order_id,
            "支付单号": payment_id,
            "买家实付": 0,
            "订单状态": "",
            "订单创建时间": group["动账时间"].min(),
            "商家备注": remark,
            "卖家实退": 0,
            "手续费": 0,
            "渠道": "企业微信",
            "确认收货时间": "",
            "打款商家金额": ""
        }

        if "确认收货时间" in group.columns:
            order_data["确认收货时间"] = group["确认收货时间"].dropna().min() if not group["确认收货时间"].dropna().empty else None

        if "打款商家金额" in group.columns:
            order_data["打款商家金额"] = group["打款商家金额"].sum() if "打款商家金额" in group.columns else 0

        for _, row in group.iterrows():
            try:
                # 安全的数值转换 - 添加数据类型检查
                def safe_amount(value):
                    """安全转换动账金额为数值"""
                    try:
                        if pd.isna(value) or value == '':
                            return 0.0
                        return float(value)
                    except (ValueError, TypeError):
                        print(f"警告: 动账金额格式错误: {value}, 类型: {type(value)}")
                        return 0.0
                
                # 保持原有逻辑，但添加安全的数值转换
                if row["动账类型"] == "收款":
                    amount = safe_amount(row["动账金额"])
                    order_data["买家实付"] += amount
                elif row["动账类型"] == "退款":
                    amount = safe_amount(row["动账金额"])
                    # 企业微信退款需要处理手续费：实际退款金额 = 显示退款金额 ÷ 0.994
                    actual_refund = abs(amount) / 0.994
                    order_data["卖家实退"] += actual_refund
                    print(f"    退款调整: 显示金额={abs(amount)}, 实际金额={actual_refund:.2f}")
                elif row["动账类型"] == "交易手续费":
                    amount = safe_amount(row["动账金额"])
                    order_data["手续费"] += abs(amount)
                print(f"  动账类型: {row['动账类型']}, 动账金额: {row['动账金额']}")
            except Exception as e:
                print(f"处理行数据时出错: {str(e)}")
                print(f"该行数据: {row}")

        output_data.append(order_data)

    result_df = pd.DataFrame(output_data)
    print(f"处理完成，结果数据行数: {len(result_df)}")
    print(f"结果数据列名: {result_df.columns.tolist()}")
    return result_df

def save_to_database(df):
    """保存数据到MySQL数据库"""
    # 定义辅助函数
    def safe_float(value):
        """安全转换数值，处理空值和非数值情况"""
        try:
            return float(value) if pd.notna(value) and value != '' else 0.0
        except (ValueError, TypeError):
            print(f"无法转换为浮点数: {value}, 类型: {type(value)}")
            return 0.0

    def safe_datetime(value):
        """安全转换日期时间"""
        try:
            if pd.notna(value) and value != '':
                dt = pd.to_datetime(value)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            return None
        except Exception as e:
            print(f"日期时间转换出错: {value}, 错误: {str(e)}")
            return None

    try:
        # 确保数据框非空
        if df.empty:
            print("错误: 数据框为空，没有数据需要保存")
            return False
        
        print("=== 数据库操作开始 ===")
        print(f"连接数据库: {DB_CONFIG['host']}, 数据库: {DB_CONFIG['database']}")
        
        # 连接数据库
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 打印数据库连接信息
        print(f"数据库连接成功: {DB_CONFIG['host']}, 数据库: {DB_CONFIG['database']}")
        
        # 验证orders表是否存在
        try:
            cursor.execute("SHOW TABLES LIKE 'orders'")
            if not cursor.fetchone():
                print("错误: 'orders'表不存在!")
                return False
            print("'orders'表存在，继续处理")
        except Exception as e:
            print(f"验证表存在时出错: {str(e)}")
        
        # 打印要保存的数据总数
        total_count = len(df)
        print(f"准备保存的数据总数: {total_count}")
        
        # 根据数据量决定分批大小
        if total_count <= 100:
            batch_size = total_count  # 小批量一次性处理
        elif total_count <= 1000:
            batch_size = 500  # 中等批量分2批
        elif total_count <= 10000:
            batch_size = 1000  # 大批量分10批
        else:
            batch_size = 2000  # 超大批量分更多批
        
        print(f"分批处理，每批大小: {batch_size}")
        
        # 批量插入语句
        insert_query = """
        INSERT INTO orders (
            order_id, payment_id, amount, status, create_time, 
            merchant_remark, refund_amount, fee, channel, 
            confirm_time, merchant_payment
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON DUPLICATE KEY UPDATE
            payment_id = VALUES(payment_id),
            amount = VALUES(amount),
            status = VALUES(status),
            create_time = VALUES(create_time),
            merchant_remark = VALUES(merchant_remark),
            refund_amount = VALUES(refund_amount),
            fee = VALUES(fee),
            channel = VALUES(channel),
            confirm_time = VALUES(confirm_time),
            merchant_payment = VALUES(merchant_payment),
            updated_at = CURRENT_TIMESTAMP
        """
        
        # 分批处理数据
        total_success = 0
        total_error = 0
        
        for batch_start in range(0, total_count, batch_size):
            batch_end = min(batch_start + batch_size, total_count)
            batch_df = df.iloc[batch_start:batch_end]
            
            print(f"处理第 {batch_start//batch_size + 1} 批，数据范围: {batch_start+1}-{batch_end}")
            
            # 准备当前批次数据
            batch_data = []
            for index, row in batch_df.iterrows():
                data = (
                    str(row['订单编号']),
                    str(row['支付单号']),
                    safe_float(row['买家实付']),
                    str(row['订单状态']),
                    safe_datetime(row['订单创建时间']),
                    str(row['商家备注']) if pd.notna(row['商家备注']) else '',
                    safe_float(row['卖家实退']),
                    safe_float(row['手续费']),
                    str(row['渠道']),
                    safe_datetime(row['确认收货时间']),
                    safe_float(row['打款商家金额'])
                )
                batch_data.append(data)
            
            try:
                # 执行批量插入
                cursor.executemany(insert_query, batch_data)
                conn.commit()
                
                batch_success = len(batch_data)
                total_success += batch_success
                print(f"第 {batch_start//batch_size + 1} 批处理完成，成功: {batch_success} 条")
                
            except Error as e:
                print(f"第 {batch_start//batch_size + 1} 批处理失败: {str(e)}")
                total_error += len(batch_data)
                # 继续处理下一批，不中断整个流程
        
        # 最终验证：只验证几条数据
        if total_success > 0:
            sample_size = min(3, total_success)
            sample_ids = [df.iloc[i]['订单编号'] for i in range(sample_size)]
            print(f"验证 {sample_size} 个订单ID:")
            
            for order_id in sample_ids:
                try:
                    check_query = "SELECT COUNT(*) FROM orders WHERE order_id = %s"
                    cursor.execute(check_query, (str(order_id),))
                    count = cursor.fetchone()[0]
                    print(f"  订单ID {order_id}: {'存在' if count > 0 else '不存在!'}")
                except Exception as e:
                    print(f"  验证订单ID {order_id} 时出错: {str(e)}")
        
        print(f"""
        数据处理完成:
        - 总数据: {total_count}
        - 成功: {total_success}
        - 失败: {total_error}
        - 成功率: {total_success/total_count*100:.1f}%
        """)
        
        return total_success > 0
        
    except Error as e:
        print(f"数据库连接或操作错误: {str(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        return False
        
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("数据库连接已关闭")
            print("=== 数据库操作结束 ===")

@app.route('/process_status/<task_id>', methods=['GET'])
def get_process_status(task_id):
    """查询处理状态"""
    if task_id in processing_tasks:
        status = processing_tasks[task_id]
        return jsonify({
            'code': 0,
            'message': '查询成功',
            'data': status
        }), 200
    else:
        return jsonify({
            'code': 1,
            'message': '任务不存在',
            'data': None
        }), 200

@app.route('/process_order', methods=['POST'])
def process_order():
    """处理订单数据的API端点"""
    try:
        print("\n===== 开始处理新的订单请求 =====")
        print(f"请求表单数据: {request.form}")
        print(f"请求文件: {request.files}")
        
        # 生成任务ID
        task_id = str(uuid.uuid4())
        processing_tasks[task_id] = {
            'status': 'processing',
            'progress': 0,
            'message': '开始处理...',
            'start_time': time.time()
        }
        
        # 检查渠道参数
        channel = request.form.get('channel')
        if not channel:
            print("错误: 未指定渠道")
            processing_tasks[task_id]['status'] = 'error'
            processing_tasks[task_id]['message'] = '未指定渠道'
            return jsonify({
                'code': 1,
                'message': '未指定渠道',
                'data': {'task_id': task_id}
            }), 200

        print(f"处理渠道: {channel}")

        # 检查订单文件
        if 'order_file' not in request.files:
            print("错误: 没有上传订单文件")
            processing_tasks[task_id]['status'] = 'error'
            processing_tasks[task_id]['message'] = '没有上传订单文件'
            return jsonify({
                'code': 1,
                'message': '没有上传订单文件',
                'data': {'task_id': task_id}
            }), 200
            
        order_file = request.files['order_file']
        if order_file.filename == '':
            print("错误: 没有选择订单文件")
            processing_tasks[task_id]['status'] = 'error'
            processing_tasks[task_id]['message'] = '没有选择订单文件'
            return jsonify({
                'code': 1,
                'message': '没有选择订单文件',
                'data': {'task_id': task_id}
            }), 200

        print(f"订单文件名: {order_file.filename}")

        # 生成唯一的订单文件名
        order_filename = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_{werkzeug.utils.secure_filename(order_file.filename)}"
        order_path = os.path.join(UPLOAD_FOLDER, order_filename)
        print(f"保存订单文件到: {order_path}")

        # 确保uploads目录存在
        if not os.path.exists(UPLOAD_FOLDER):
            try:
                os.makedirs(UPLOAD_FOLDER, mode=0o777)
                print(f"创建上传目录: {UPLOAD_FOLDER}")
            except Exception as e:
                print(f"无法创建上传目录: {str(e)}")
                processing_tasks[task_id]['status'] = 'error'
                processing_tasks[task_id]['message'] = f'无法创建上传目录: {str(e)}'
                return jsonify({
                    'code': 1,
                    'message': f'无法创建上传目录: {str(e)}',
                    'data': {'task_id': task_id}
                }), 200

        # 保存订单文件
        order_file.save(order_path)
        print(f"订单文件已保存: {order_path}")

        try:
            # 根据渠道类型处理数据
            if channel == '支付宝':
                print("开始处理支付宝渠道数据...")
                df = process_alipay(order_path)
            elif channel == '企业微信':
                print("开始处理企业微信渠道数据...")
                try:
                    # 使用with语句确保文件句柄正确释放
                    with pd.ExcelFile(order_path) as xl:
                        sheet_names = xl.sheet_names
                        print(f"Excel文件中的工作表: {sheet_names}")
                        
                        if not sheet_names:
                            raise ValueError("Excel文件中没有找到任何工作表")
                        
                        # 使用第一个工作表
                        first_sheet = sheet_names[0]
                        print(f"使用工作表: {first_sheet}")
                        raw_df = xl.parse(first_sheet)
                        print(f"成功读取Excel文件，行数: {len(raw_df)}")
                        df = process_wechat(raw_df)
                except Exception as e:
                    print(f"读取或处理企业微信数据时出错: {str(e)}")
                    import traceback
                    print(f"错误堆栈: {traceback.format_exc()}")
                    processing_tasks[task_id]['status'] = 'error'
                    processing_tasks[task_id]['message'] = f'读取或处理企业微信数据时出错: {str(e)}'
                    return jsonify({
                        'code': 1,
                        'message': f'读取或处理企业微信数据时出错: {str(e)}',
                        'data': {'task_id': task_id}
                    }), 200
            elif channel in ['天猫', '淘宝']:
                print(f"开始处理{channel}渠道数据...")
                df = process_tmall_taobao(channel, order_file)
            else:
                print(f"不支持的渠道类型: {channel}")
                processing_tasks[task_id]['status'] = 'error'
                processing_tasks[task_id]['message'] = f'不支持的渠道类型: {channel}'
                return jsonify({
                    'code': 1,
                    'message': f'不支持的渠道类型: {channel}',
                    'data': {'task_id': task_id}
                }), 200

            print(f"数据处理完成，准备保存到数据库，数据行数: {len(df)}")
            # 保存到数据库
            if save_to_database(df):
                # 根据原始文件格式决定输出文件名和格式
                if order_file.filename.lower().endswith('.csv'):
                    # 如果是CSV文件，生成CSV格式的处理后文件
                    output_filename = f'processed_{order_filename.replace(".csv", "")}.csv'
                    output_path = os.path.join(UPLOAD_FOLDER, output_filename)
                    print(f"准备生成处理后的CSV文件: {output_path}")
                    
                    try:
                        df.to_csv(output_path, index=False, encoding='utf-8-sig')  # 使用UTF-8-BOM编码确保中文正确显示
                        print(f"处理后的CSV文件已生成: {output_path}")
                    except Exception as e:
                        print(f"生成CSV文件失败: {str(e)}")
                        processing_tasks[task_id]['status'] = 'error'
                        processing_tasks[task_id]['message'] = f'数据已保存到数据库，但CSV文件生成失败: {str(e)}'
                        return jsonify({
                            'code': 1,
                            'message': f'数据已保存到数据库，但CSV文件生成失败: {str(e)}',
                            'data': {'task_id': task_id}
                        }), 200
                else:
                    # 如果是Excel文件，生成Excel格式的处理后文件
                    output_filename = f'processed_{order_filename.replace(".xlsx", "").replace(".xls", "")}.xlsx'
                    output_path = os.path.join(UPLOAD_FOLDER, output_filename)
                    print(f"准备生成处理后的Excel文件: {output_path}")
                    
                    try:
                        df.to_excel(output_path, index=False)
                        print(f"处理后的Excel文件已生成: {output_path}")
                    except Exception as e:
                        print(f"生成Excel文件失败: {str(e)}")
                        processing_tasks[task_id]['status'] = 'error'
                        processing_tasks[task_id]['message'] = f'数据已保存到数据库，但Excel文件生成失败: {str(e)}'
                        return jsonify({
                            'code': 1,
                            'message': f'数据已保存到数据库，但Excel文件生成失败: {str(e)}',
                            'data': {'task_id': task_id}
                        }), 200

                processing_tasks[task_id]['status'] = 'success'
                processing_tasks[task_id]['message'] = '处理成功，数据已保存到数据库'
                processing_tasks[task_id]['data'] = {'processed_file': output_filename}
                return jsonify({
                    'code': 0,
                    'message': '处理成功，数据已保存到数据库',
                    'data': {'task_id': task_id}
                }), 200
            else:
                print("数据库保存失败")
                processing_tasks[task_id]['status'] = 'error'
                processing_tasks[task_id]['message'] = '数据库保存失败'
                return jsonify({
                    'code': 1,
                    'message': '数据库保存失败',
                    'data': {'task_id': task_id}
                }), 200

        finally:
            # 清理订单文件 - 添加延迟和重试机制
            if os.path.exists(order_path):
                try:
                    # 等待一小段时间确保文件句柄完全释放
                    time.sleep(0.5)
                    os.remove(order_path)
                    print(f"临时订单文件已删除: {order_path}")
                except PermissionError as e:
                    print(f"文件删除失败，可能仍在被使用: {str(e)}")
                    # 不抛出异常，让程序继续运行
                except Exception as e:
                    print(f"删除文件时出错: {str(e)}")

    except Exception as e:
        print(f"处理订单时发生异常: {str(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        processing_tasks[task_id]['status'] = 'error'
        processing_tasks[task_id]['message'] = str(e)
        return jsonify({
            'code': 1,
            'message': str(e),
            'data': {'task_id': task_id}
        }), 200

def process_tmall_taobao(channel, order_file):
    """处理天猫和淘宝渠道的订单数据
    
    支持天猫和淘宝多个店铺的数据处理，通过入参方式指定渠道
    """
    # 验证退款文件
    if 'refund_order_file' not in request.files:
        raise ValueError(f'{channel}渠道需要上传退款文件')
    
    refund_file = request.files['refund_order_file']
    if refund_file.filename == '':
        raise ValueError('没有选择退款文件')
    
    # 验证是否有且仅有一个文件包含"退款"
    order_is_refund = '退款' in order_file.filename
    refund_is_refund = '退款' in refund_file.filename
    
    if order_is_refund and refund_is_refund:
        raise ValueError('只能有一个退款文件')
    elif not order_is_refund and not refund_is_refund:
        raise ValueError('必须上传一个退款文件')
    
    # 如果订单文件是退款文件，则交换两个文件
    if order_is_refund:
        order_file, refund_file = refund_file, order_file

    # 保存退款文件到磁盘
    refund_filename = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_refund_{werkzeug.utils.secure_filename(refund_file.filename)}"
    refund_path = os.path.join(UPLOAD_FOLDER, refund_filename)
    print(f"保存退款文件到: {refund_path}")
    refund_file.save(refund_path)
    
    print(f"检查退款文件是否存在: {os.path.exists(refund_path)}")
    print(f"退款文件大小: {os.path.getsize(refund_path) if os.path.exists(refund_path) else '不存在'}")

    try:
        print("开始读取订单数据...")
        # 读取订单文件 - 使用传入的order_file而不是order_path
        order_df = pd.read_excel(order_file, dtype={'订单编号': str}, engine='openpyxl')
        print(f"订单数据读取成功，行数: {len(order_df)}")
        
        print("开始读取退款数据...")
        # 尝试使用不同的引擎读取退款文件
        try:
            print("尝试使用xlrd引擎读取退款文件...")
            refund_df = pd.read_excel(refund_path, dtype={'订单编号': str}, engine='xlrd')
        except Exception as e:
            print(f"使用xlrd引擎读取失败: {str(e)}")
            print("尝试使用openpyxl引擎读取...")
            try:
                refund_df = pd.read_excel(refund_path, dtype={'订单编号': str}, engine='openpyxl')
            except Exception as e2:
                print(f"使用openpyxl引擎读取失败: {str(e2)}")
                print("尝试使用pandas默认引擎读取...")
                try:
                    refund_df = pd.read_excel(refund_path, dtype={'订单编号': str})
                except Exception as e3:
                    print(f"pandas默认引擎读取失败: {str(e3)}")
                    print("尝试读取CSV格式...")
                    try:
                        # 尝试直接读取为CSV
                        refund_df = pd.read_csv(refund_path, dtype={'订单编号': str})
                    except Exception as e4:
                        print(f"CSV读取失败: {str(e4)}")
                        raise ValueError(f"无法读取退款文件，请检查文件格式。支持格式：.xlsx, .xls, .csv。错误信息: {str(e)} | {str(e2)} | {str(e3)} | {str(e4)}")
        
        # 使用统一的处理模块
        from yi_process import process_orders, update_with_refunds
        print(f"开始处理{channel}数据...")
        
        # 处理订单数据
        intermediate_df = process_orders(order_df)
        
        # 处理退款数据
        result_df = update_with_refunds(intermediate_df, refund_df)
        
        # 设置渠道为天猫或淘宝
        result_df['渠道'] = channel
        
        print(f"{channel}数据处理完成")
        return result_df
        
    except Exception as e:
        print(f"处理{channel}数据时出错: {str(e)}")
        print(f"错误类型: {type(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        raise ValueError(f"{channel}数据处理失败: {str(e)}")
    finally:
        # 清理退款文件
        if os.path.exists(refund_path):
            os.remove(refund_path)
            print(f"临时退款文件已删除: {refund_path}")

if __name__ == '__main__':
    # 设置Flask应用的超时配置
    app.config['SEND_FILE_MAX_AGE_DEFAULT'] = 0
    # 增加请求超时时间，支持大量数据处理
    app.config['PERMANENT_SESSION_LIFETIME'] = 300  # 5分钟
    # 设置请求体大小限制
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB
    
    # 启动服务器
    app.run(host='0.0.0.0', port=5000, threaded=True) 