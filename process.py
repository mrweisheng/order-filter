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
    'host': 'localhost',
    'user': 'root',
    'password': '1qaz!QAZ2wsx@WSX',
    'database': 'wenzhi'
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
    output_data = []
    
    # 过滤掉动账类型为提现的记录
    df = df[df['动账类型'] != '提现']
    
    # 按"关联单号"分组
    grouped = df.groupby("关联单号")

    for order_id, group in grouped:
        order_data = {
            "订单编号": order_id,
            "支付单号": group["商户单号"].iloc[0],
            "买家实付": 0,
            "订单状态": "",
            "订单创建时间": group["动账时间"].min(),
            "商家备注": group["备注"].iloc[0],
            "卖家实退": 0,
            "手续费": 0,
            "渠道": "企业微信",
            "确认收货时间": "",
            "打款商家金额": ""
        }

        if "确认收货时间" in group.columns:
            order_data["确认收货时间"] = group["确认收货时间"].dropna().min()

        if "打款商家金额" in group.columns:
            order_data["打款商家金额"] = group["打款商家金额"].sum()

        for _, row in group.iterrows():
            if row["动账类型"] == "收款":
                order_data["买家实付"] += row["动账金额"]
            elif row["动账类型"] == "退款":
                order_data["卖家实退"] += abs(row["动账金额"])
            elif row["动账类型"] == "交易手续费":
                order_data["手续费"] += abs(row["动账金额"])

        output_data.append(order_data)

    return pd.DataFrame(output_data)

def save_to_database(df):
    """保存数据到MySQL数据库"""
    # 定义辅助函数
    def safe_float(value):
        """安全转换数值，处理空值和非数值情况"""
        try:
            return float(value) if pd.notna(value) and value != '' else 0.0
        except (ValueError, TypeError):
            return 0.0

    def safe_datetime(value):
        """安全转换日期时间"""
        try:
            if pd.notna(value) and value != '':
                dt = pd.to_datetime(value)
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            return None
        except:
            return None

    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # 打印要保存的数据总数
        print(f"准备保存的数据总数: {len(df)}")
        
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
        """
        
        success_count = 0
        error_count = 0
        
        # 准备数据并执行插入
        for index, row in df.iterrows():
            try:
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
                cursor.execute(insert_query, data)
                success_count += 1
                
                # 每100条数据提交一次
                if success_count % 100 == 0:
                    conn.commit()
                    print(f"已成功处理 {success_count} 条数据")
                
            except Error as e:
                error_count += 1
                print(f"第 {index} 行数据插入失败: {str(e)}")
                print(f"失败的数据: {data}")
        
        # 最后提交剩余的数据
        conn.commit()
        
        print(f"""
        数据处理完成:
        - 总数据: {len(df)}
        - 成功: {success_count}
        - 失败: {error_count}
        """)
        
        return True
        
    except Error as e:
        print(f"数据库错误: {str(e)}")
        return False
        
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

@app.route('/process_order', methods=['POST'])
def process_order():
    """处理订单数据的API端点"""
    try:
        # 检查渠道参数
        channel = request.form.get('channel')
        if not channel:
            return jsonify({
                'code': 1,  # 改为1表示失败
                'message': '未指定渠道',
                'data': None
            }), 200  # HTTP状态码统一为200

        # 检查订单文件
        if 'order_file' not in request.files:
            return jsonify({
                'code': 1,
                'message': '没有上传订单文件',
                'data': None
            }), 200
            
        order_file = request.files['order_file']
        if order_file.filename == '':
            return jsonify({'error': '没有选择订单文件'}), 400

        # 生成唯一的订单文件名
        order_filename = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_{werkzeug.utils.secure_filename(order_file.filename)}"
        order_path = os.path.join(UPLOAD_FOLDER, order_filename)

        # 确保uploads目录存在
        if not os.path.exists(UPLOAD_FOLDER):
            try:
                os.makedirs(UPLOAD_FOLDER, mode=0o777)
            except Exception as e:
                return jsonify({'error': f'无法创建上传目录: {str(e)}'}), 500

        # 保存订单文件
        order_file.save(order_path)

        try:
            if channel == '支付宝':
                df = process_alipay(order_path)
            elif channel == '企业微信':
                raw_df = pd.read_excel(order_path, sheet_name="result")
                df = process_wechat(raw_df)
            elif channel == '慧辞':
                try:
                    # 打印订单文件名
                    print(f"订单文件名: {order_file.filename}")
                    
                    # 验证订单文件名是否包含"慧辞"
                    if '慧辞' not in order_file.filename:
                        return jsonify({
                            'code': 1,
                            'message': f'订单文件名必须包含"慧辞"，当前文件名: {order_file.filename}',
                            'data': None
                        }), 200  # 改为200
                    
                    # 验证退款文件
                    if 'refund_order_file' not in request.files:
                        return jsonify({
                            'code': 1,
                            'message': '慧辞渠道需要上传退款文件',
                            'data': None
                        }), 200
                    
                    refund_file = request.files['refund_order_file']
                    if refund_file.filename == '':
                        return jsonify({
                            'code': 1,
                            'message': '没有选择退款文件',
                            'data': None
                        }), 200
                    
                    # 验证退款文件名是否包含"慧辞"
                    if '慧辞' not in refund_file.filename:
                        return jsonify({
                            'code': 1,
                            'message': '退款文件名必须包含"慧辞"',
                            'data': None
                        }), 200
                    
                    # 验证是否有且仅有一个文件包含"退款"
                    order_is_refund = '退款' in order_file.filename
                    refund_is_refund = '退款' in refund_file.filename
                    
                    if order_is_refund and refund_is_refund:
                        return jsonify({
                            'code': 1,
                            'message': '只能有一个退款文件',
                            'data': None
                        }), 200
                    elif not order_is_refund and not refund_is_refund:
                        return jsonify({
                            'code': 1,
                            'message': '必须上传一个退款文件',
                            'data': None
                        }), 200
                    
                    # 如果订单文件是退款文件，则交换两个文件
                    if order_is_refund:
                        order_file, refund_file = refund_file, order_file

                    try:
                        # 直接从内存中读取文件内容
                        order_df = pd.read_excel(order_file, dtype={'订单编号': str}, engine='openpyxl')
                        refund_df = pd.read_excel(refund_file, dtype={'订单编号': str}, engine='openpyxl')
                        
                        # 调用处理函数
                        from huici_process import process_orders, update_with_refunds
                        print("开始处理慧辞数据...")
                        
                        # 处理订单数据
                        intermediate_df = process_orders(order_df)
                        
                        # 处理退款数据
                        df = update_with_refunds(intermediate_df, refund_df)
                        df['渠道'] = '慧辞'
                        
                        print("慧辞数据处理完成")
                        
                    except Exception as e:
                        print(f"处理慧辞数据时出错: {str(e)}")
                        print(f"错误类型: {type(e)}")
                        import traceback
                        print(f"错误堆栈: {traceback.format_exc()}")
                        raise
                    
                except Exception as e:
                    print(f"慧辞渠道处理错误: {str(e)}")
                    return jsonify({'error': str(e)}), 500
            elif channel == '匠易艺':
                try:
                    # 验证订单文件名是否包含"匠易艺"
                    if '匠易艺' not in order_file.filename:
                        return jsonify({'error': '订单文件名必须包含"匠易艺"'}), 400
                    
                    # 验证退款文件
                    if 'refund_order_file' not in request.files:
                        return jsonify({'error': '匠易艺渠道需要上传退款文件'}), 400
                    
                    refund_file = request.files['refund_order_file']
                    if refund_file.filename == '':
                        return jsonify({'error': '没有选择退款文件'}), 400
                    
                    # 验证退款文件名是否包含"匠易艺"
                    if '匠易艺' not in refund_file.filename:
                        return jsonify({'error': '退款文件名必须包含"匠易艺"'}), 400
                    
                    # 验证是否有且仅有一个文件包含"退款"
                    order_is_refund = '退款' in order_file.filename
                    refund_is_refund = '退款' in refund_file.filename
                    
                    if order_is_refund and refund_is_refund:
                        return jsonify({'error': '只能有一个退款文件'}), 400
                    elif not order_is_refund and not refund_is_refund:
                        return jsonify({'error': '必须上传一个退款文件'}), 400
                    
                    # 如果订单文件是退款文件，则交换两个文件
                    if order_is_refund:
                        order_file, refund_file = refund_file, order_file

                    try:
                        # 直接从内存中读取文件内容
                        order_df = pd.read_excel(order_file, dtype={'订单编号': str}, engine='openpyxl')
                        refund_df = pd.read_excel(refund_file, dtype={'订单编号': str}, engine='openpyxl')
                        
                        # 调用处理函数
                        from yi_process import process_orders, update_with_refunds
                        print("开始处理匠易艺数据...")
                        
                        # 处理订单数据
                        intermediate_df = process_orders(order_df)
                        
                        # 处理退款数据
                        df = update_with_refunds(intermediate_df, refund_df)
                        
                        print("匠易艺数据处理完成")
                        
                    except Exception as e:
                        print(f"处理匠易艺数据时出错: {str(e)}")
                        print(f"错误类型: {type(e)}")
                        import traceback
                        print(f"错误堆栈: {traceback.format_exc()}")
                        raise
                    
                except Exception as e:
                    print(f"匠易艺渠道处理错误: {str(e)}")
                    return jsonify({'error': str(e)}), 500
            else:
                # 其他渠道（匠易艺）需要退款文件
                if 'refund_order_file' not in request.files:
                    return jsonify({'error': f'{channel}渠道需要上传退款文件'}), 400
                
                refund_file = request.files['refund_order_file']
                if refund_file.filename == '':
                    return jsonify({'error': '没有选择退款文件'}), 400

                # 生成唯一的退款文件名
                refund_filename = f"{int(time.time())}_{uuid.uuid4().hex[:8]}_{werkzeug.utils.secure_filename(refund_file.filename)}"
                refund_path = os.path.join(UPLOAD_FOLDER, refund_filename)
                
                # 保存退款文件
                refund_file.save(refund_path)

                try:
                    if channel == '匠易艺':
                        from yi import main as process_yi
                        df = process_yi(order_path, refund_path)
                    else:
                        return jsonify({'error': f'不支持的渠道类型: {channel}'}), 400
                finally:
                    # 清理退款文件
                    if os.path.exists(refund_path):
                        os.remove(refund_path)

            # 保存到数据库
            if save_to_database(df):
                # 生成处理后的文件名
                output_filename = f'processed_{order_filename}'
                output_path = os.path.join(UPLOAD_FOLDER, output_filename)
                
                try:
                    df.to_excel(output_path, index=False)
                except Exception as e:
                    return jsonify({
                        'message': '数据已保存到数据库，但Excel文件生成失败',
                        'error_detail': str(e)
                    }), 200

                return jsonify({
                    'code': 0,  # 改为0表示成功
                    'message': '处理成功，数据已保存到数据库',
                    'data': {
                        'processed_file': output_filename
                    }
                }), 200
            else:
                return jsonify({'error': '数据库保存失败'}), 500

        finally:
            # 清理订单文件
            if os.path.exists(order_path):
                os.remove(order_path)

    except Exception as e:
        return jsonify({
            'code': 1,
            'message': str(e),
            'data': None
        }), 200  # 错误时也返回200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000) 