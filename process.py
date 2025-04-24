import pandas as pd
import os
from flask import Flask, request, jsonify
import werkzeug.utils
import mysql.connector
from mysql.connector import Error
import uuid
import time
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

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
    # 读取Excel文件并跳过前两行和最后一行
    df = pd.read_excel(file_path, skiprows=2, engine='openpyxl')
    # 移除最后一行
    df = df[:-1]

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

    return new_df

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
        
        order_data = {
            "订单编号": order_id,
            "支付单号": group["商户单号"].iloc[0],
            "买家实付": 0,
            "订单状态": "",
            "订单创建时间": group["动账时间"].min(),
            "商家备注": group["备注"].iloc[0] if "备注" in group.columns else "",
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
                if row["动账类型"] == "收款":
                    order_data["买家实付"] += row["动账金额"]
                elif row["动账类型"] == "退款":
                    order_data["卖家实退"] += abs(row["动账金额"])
                elif row["动账类型"] == "交易手续费":
                    order_data["手续费"] += abs(row["动账金额"])
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
        
        # 尝试获取已有的订单数量
        try:
            conn_test = mysql.connector.connect(**DB_CONFIG)
            cursor_test = conn_test.cursor()
            cursor_test.execute("SELECT COUNT(*) FROM orders")
            count_before = cursor_test.fetchone()[0]
            print(f"数据库操作前orders表中的记录数: {count_before}")
            cursor_test.close()
            conn_test.close()
        except Exception as e:
            print(f"获取现有订单数量时出错: {str(e)}")
        
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
        
        # 打印要保存的数据总数和部分样本
        print(f"准备保存的数据总数: {len(df)}")
        if not df.empty:
            print(f"数据样本 (前3行):")
            for i, row in df.head(3).iterrows():
                print(f"  行 {i}: 订单编号={row['订单编号']}, 渠道={row['渠道']}, 金额={row['买家实付']}")
        
        # 修改插入语句，更新时自动更新updated_at
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
            -- 不更新customer_id和writer_id字段，保留原值
        """
        
        # 确认表结构
        try:
            cursor.execute("DESCRIBE orders")
            columns = cursor.fetchall()
            print("orders表结构:")
            for col in columns:
                print(f"  {col[0]}: {col[1]}")
        except Exception as e:
            print(f"获取表结构时出错: {str(e)}")
        
        success_count = 0
        error_count = 0
        
        # 保存一份订单ID列表，用于后续验证
        order_ids = df['订单编号'].tolist()
        
        # 准备数据并执行插入
        for index, row in df.iterrows():
            try:
                # 打印每一行要插入的数据
                print(f"处理第 {index+1}/{len(df)} 行数据，订单编号: {row['订单编号']}")
                
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
                
                # 打印SQL和参数
                # print(f"SQL: {insert_query}")
                print(f"参数: {data}")
                
                cursor.execute(insert_query, data)
                success_count += 1
                
                # 每10条数据提交一次
                if success_count % 10 == 0:
                    conn.commit()
                    print(f"已成功处理 {success_count}/{len(df)} 条数据, 已提交")
                    
                    # 验证提交的数据是否存在
                    if success_count == 10:  # 只在第一次提交后检查
                        try:
                            check_query = f"SELECT COUNT(*) FROM orders WHERE order_id = %s"
                            cursor.execute(check_query, (str(row['订单编号']),))
                            count = cursor.fetchone()[0]
                            if count > 0:
                                print(f"验证成功: 订单 {row['订单编号']} 已存在于数据库中")
                            else:
                                print(f"验证失败: 订单 {row['订单编号']} 不存在于数据库中!")
                        except Exception as e:
                            print(f"验证数据时出错: {str(e)}")
                
            except Error as e:
                error_count += 1
                print(f"第 {index+1} 行数据插入失败: {str(e)}")
                print(f"失败的数据: {data}")
                import traceback
                print(f"错误堆栈: {traceback.format_exc()}")
        
        # 最后提交剩余的数据
        try:
            conn.commit()
            print("最终提交完成")
            
            # 最终验证：随机选择几个订单ID进行检查
            import random
            if order_ids:
                sample_size = min(5, len(order_ids))
                sample_ids = random.sample(order_ids, sample_size)
                print(f"随机抽查 {sample_size} 个订单ID:")
                
                for order_id in sample_ids:
                    try:
                        check_query = "SELECT COUNT(*) FROM orders WHERE order_id = %s"
                        cursor.execute(check_query, (str(order_id),))
                        count = cursor.fetchone()[0]
                        print(f"  订单ID {order_id}: {'存在' if count > 0 else '不存在!'}")
                    except Exception as e:
                        print(f"  验证订单ID {order_id} 时出错: {str(e)}")
                
                # 检查渠道数据
                channel_value = df['渠道'].iloc[0] if not df.empty else '未知'
                try:
                    check_query = "SELECT COUNT(*) FROM orders WHERE channel = %s"
                    cursor.execute(check_query, (channel_value,))
                    count = cursor.fetchone()[0]
                    print(f"渠道 '{channel_value}' 的记录总数: {count}")
                except Exception as e:
                    print(f"验证渠道数据时出错: {str(e)}")
                
            # 查询数据库总记录数
            try:
                cursor.execute("SELECT COUNT(*) FROM orders")
                count_after = cursor.fetchone()[0]
                print(f"数据库操作后orders表中的记录数: {count_after}")
                if 'count_before' in locals():
                    print(f"新增记录数: {count_after - count_before}")
            except Exception as e:
                print(f"获取最终记录数时出错: {str(e)}")
            
        except Error as e:
            print(f"最终提交时出错: {str(e)}")
            import traceback
            print(f"错误堆栈: {traceback.format_exc()}")
        
        print(f"""
        数据处理完成:
        - 总数据: {len(df)}
        - 成功: {success_count}
        - 失败: {error_count}
        """)
        
        return success_count > 0
        
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

@app.route('/process_order', methods=['POST'])
def process_order():
    """处理订单数据的API端点"""
    try:
        print("\n===== 开始处理新的订单请求 =====")
        print(f"请求表单数据: {request.form}")
        print(f"请求文件: {request.files}")
        
        # 检查渠道参数
        channel = request.form.get('channel')
        if not channel:
            print("错误: 未指定渠道")
            return jsonify({
                'code': 1,
                'message': '未指定渠道',
                'data': None
            }), 200

        print(f"处理渠道: {channel}")

        # 检查订单文件
        if 'order_file' not in request.files:
            print("错误: 没有上传订单文件")
            return jsonify({
                'code': 1,
                'message': '没有上传订单文件',
                'data': None
            }), 200
            
        order_file = request.files['order_file']
        if order_file.filename == '':
            print("错误: 没有选择订单文件")
            return jsonify({
                'code': 1,
                'message': '没有选择订单文件',
                'data': None
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
                return jsonify({
                    'code': 1,
                    'message': f'无法创建上传目录: {str(e)}',
                    'data': None
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
                    print(f"尝试读取Excel文件，sheet名: result")
                    raw_df = pd.read_excel(order_path, sheet_name="result")
                    print(f"成功读取Excel文件，行数: {len(raw_df)}")
                    df = process_wechat(raw_df)
                except Exception as e:
                    print(f"读取或处理企业微信数据时出错: {str(e)}")
                    import traceback
                    print(f"错误堆栈: {traceback.format_exc()}")
                    raise
            elif channel in ['天猫', '淘宝']:
                print(f"开始处理{channel}渠道数据...")
                df = process_tmall_taobao(channel, order_file)
            else:
                print(f"不支持的渠道类型: {channel}")
                return jsonify({
                    'code': 1,
                    'message': f'不支持的渠道类型: {channel}',
                    'data': None
                }), 200

            print(f"数据处理完成，准备保存到数据库，数据行数: {len(df)}")
            # 保存到数据库
            if save_to_database(df):
                # 生成处理后的文件名
                output_filename = f'processed_{order_filename}'
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)
                print(f"准备生成处理后的Excel文件: {output_path}")
                
                try:
                    df.to_excel(output_path, index=False)
                    print(f"处理后的Excel文件已生成: {output_path}")
                except Exception as e:
                    print(f"生成Excel文件失败: {str(e)}")
                    return jsonify({
                        'code': 1,
                        'message': f'数据已保存到数据库，但Excel文件生成失败: {str(e)}',
                        'data': None
                    }), 200

                print("处理成功，返回成功响应")
                return jsonify({
                    'code': 0,
                    'message': '处理成功，数据已保存到数据库',
                    'data': {
                        'processed_file': output_filename
                    }
                }), 200
            else:
                print("数据库保存失败")
                return jsonify({
                    'code': 1,
                    'message': '数据库保存失败',
                    'data': None
                }), 200

        finally:
            # 清理订单文件
            if os.path.exists(order_path):
                os.remove(order_path)
                print(f"临时订单文件已删除: {order_path}")

    except Exception as e:
        print(f"处理订单时发生异常: {str(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        return jsonify({
            'code': 1,
            'message': str(e),
            'data': None
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
            print("尝试将文件转换为CSV然后读取...")
            # 尝试使用shell命令将Excel转换为CSV
            import subprocess
            try:
                # 创建一个临时CSV文件
                csv_path = refund_path.replace('.xlsx', '.csv')
                # 使用python直接打开Excel并另存为CSV
                import win32com.client
                excel = win32com.client.Dispatch("Excel.Application")
                excel.Visible = False
                workbook = excel.Workbooks.Open(os.path.abspath(refund_path))
                workbook.SaveAs(os.path.abspath(csv_path), FileFormat=6)  # 6 为CSV格式
                workbook.Close()
                excel.Quit()
                
                # 读取转换后的CSV
                print(f"尝试读取转换后的CSV文件: {csv_path}")
                refund_df = pd.read_csv(csv_path, dtype={'订单编号': str})
                
                # 删除临时CSV文件
                if os.path.exists(csv_path):
                    os.remove(csv_path)
                    print(f"临时CSV文件已删除: {csv_path}")
            except Exception as e2:
                print(f"转换为CSV失败: {str(e2)}")
                raise ValueError(f"无法读取退款文件，请检查文件格式: {str(e)} | {str(e2)}")
        
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
    app.run(host='0.0.0.0', port=5000) 