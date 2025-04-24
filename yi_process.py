import pandas as pd

def process_orders(order_df):
    """处理订单数据"""
    try:
        print(f"原始订单数据行数: {len(order_df)}")
        print(f"订单文件的实际列名: {order_df.columns.tolist()}")
        
        # 列名映射字典 - 支持不同版本的列名
        column_mappings = {
            # 标准列名
            '订单编号': ['订单编号', '订单号', '交易号'],
            '支付单号': ['支付单号', '支付号', '交易单号'],
            '买家实际支付金额': ['买家实际支付金额', '买家实付金额', '实付金额', '买家实付', '付款金额', '总金额'],
            '订单状态': ['订单状态', '状态'],
            '订单创建时间': ['订单创建时间', '创建时间', '交易创建时间'],
            '商家备注': ['商家备注', '备注', '卖家备注'],
            '确认收货时间': ['确认收货时间', '买家确认收货时间'],
            '打款商家金额': ['打款商家金额', '实收金额', '商家实收', '确认收货打款金额'], 
            '卖家服务费': ['卖家服务费', '服务费'],
            '买家服务费': ['买家服务费', '客户服务费']
        }
        
        # 找出每列在原始数据中对应的列名
        column_mapping_result = {}
        for target_col, possible_names in column_mappings.items():
            found = False
            for col_name in possible_names:
                if col_name in order_df.columns:
                    column_mapping_result[target_col] = col_name
                    found = True
                    break
            
            if not found:
                # 如果是必要的列但找不到，给出警告
                if target_col in ['订单编号', '支付单号', '买家实际支付金额', '订单状态', '订单创建时间']:
                    print(f"警告: 找不到必要的列 '{target_col}'")
                
                # 对于非必要列，使用默认值
                if target_col == '商家备注':
                    column_mapping_result[target_col] = None
                elif target_col in ['卖家服务费', '买家服务费', '打款商家金额', '确认收货时间']:
                    column_mapping_result[target_col] = None
        
        print(f"列映射结果: {column_mapping_result}")
        
        # 提取订单文件中的必要字段，使用找到的列名
        cols_to_extract = []
        for target_col, orig_col in column_mapping_result.items():
            if orig_col is not None:
                cols_to_extract.append(orig_col)
        
        # 只保留存在的列
        extracted_df = order_df[cols_to_extract]
        print(f"提取字段后订单数据行数: {len(extracted_df)}")
        
        # 创建新的DataFrame，只包含需要的列，并按规则处理数据
        new_data = {}
        
        # 添加标准列
        for target_col, orig_col in column_mapping_result.items():
            if orig_col is not None:
                new_data[target_col] = extracted_df[orig_col]
            else:
                # 处理缺失的列
                if target_col == '商家备注':
                    new_data[target_col] = ''
                elif target_col == '确认收货时间':
                    new_data[target_col] = None
                elif target_col in ['卖家服务费', '买家服务费', '打款商家金额']:
                    new_data[target_col] = 0
                elif target_col == '买家实际支付金额' and '总金额' in order_df.columns:
                    # 特殊处理：如果没有买家实际支付金额但有总金额，使用总金额
                    new_data[target_col] = order_df['总金额']
        
        # 创建新的DataFrame
        new_df = pd.DataFrame(new_data)
        
        # 确保数值字段为数值类型，并处理缺失值
        numeric_cols = []
        if '买家实际支付金额' in new_df.columns:
            numeric_cols.append('买家实际支付金额')
        if '卖家服务费' in new_df.columns:
            numeric_cols.append('卖家服务费')
        if '买家服务费' in new_df.columns:
            numeric_cols.append('买家服务费')
        
        for col in numeric_cols:
            new_df[col] = pd.to_numeric(new_df[col], errors='coerce').fillna(0)
            print(f"{col} 非空值数量: {new_df[col].notna().sum()}")
        
        # 计算手续费 (如果存在相关列)
        if '卖家服务费' in new_df.columns and '买家服务费' in new_df.columns:
            new_df['手续费'] = new_df['卖家服务费'].fillna(0) + new_df['买家服务费'].fillna(0)
        else:
            # 如果没有服务费列，则设置为0
            new_df['手续费'] = 0
        
        # 选择最终需要的字段
        result_columns = ['订单编号', '支付单号', '买家实际支付金额', '订单状态', 
                        '订单创建时间', '商家备注', '确认收货时间', '打款商家金额', '手续费']
        
        # 确保所有列都存在
        for col in result_columns:
            if col not in new_df.columns:
                if col in ['订单编号', '支付单号', '买家实际支付金额', '订单状态', '订单创建时间']:
                    # 必需列缺失会导致错误
                    raise ValueError(f"缺少必要的列: {col}")
                # 非必需列可以设置默认值
                if col == '商家备注':
                    new_df[col] = ''
                elif col == '确认收货时间':
                    new_df[col] = None
                elif col in ['打款商家金额', '手续费']:
                    new_df[col] = 0
        
        intermediate_df = new_df[result_columns]
        
        print(f"最终处理后订单数据行数: {len(intermediate_df)}")
        return intermediate_df
        
    except Exception as e:
        print(f"处理订单数据时出错: {str(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        raise

def update_with_refunds(intermediate_df, refund_df):
    """处理退款数据"""
    try:
        print(f"订单数据行数: {len(intermediate_df)}")
        print(f"退款数据行数: {len(refund_df)}")
        print(f"退款文件的实际列名: {refund_df.columns.tolist()}")
        
        # 列名映射
        refund_column_mappings = {
            '订单编号': ['订单编号', '订单号', '交易号'],
            '买家实际支付金额': ['买家实际支付金额', '买家实付金额', '实付金额', '付款金额', '总金额'],
            '买家退款金额': ['买家退款金额', '退款金额', '退款'],
            '订单付款时间': ['订单付款时间', '付款时间', '支付时间', '创建时间']
        }
        
        # 找出每列在原始数据中对应的列名
        refund_mapping_result = {}
        for target_col, possible_names in refund_column_mappings.items():
            for col_name in possible_names:
                if col_name in refund_df.columns:
                    refund_mapping_result[target_col] = col_name
                    break
            
            # 如果找不到列，且是必要的列，给出警告
            if target_col not in refund_mapping_result:
                if target_col in ['订单编号', '买家退款金额']:
                    print(f"警告: 退款文件中找不到必要的列 '{target_col}'")
                    # 尝试其他替代方法或使用默认值
        
        print(f"退款列映射结果: {refund_mapping_result}")
        
        # 提取退款文件中的必要字段
        refund_cols_to_extract = []
        for target_col, orig_col in refund_mapping_result.items():
            refund_cols_to_extract.append(orig_col)
        
        # 只提取存在的列
        try:
            extracted_refund = refund_df[refund_cols_to_extract]
        except KeyError as e:
            print(f"提取退款数据列时出错: {str(e)}")
            # 如果某些列不存在，创建一个最小的DataFrame
            if '订单编号' in refund_mapping_result and '买家退款金额' in refund_mapping_result:
                extracted_refund = refund_df[[
                    refund_mapping_result['订单编号'],
                    refund_mapping_result['买家退款金额']
                ]]
            else:
                # 如果连必要的列都没有，则创建一个空DataFrame
                print("退款文件缺少必要的列，将创建空DataFrame")
                extracted_refund = pd.DataFrame(columns=['订单编号', '买家退款金额'])
        
        # 创建标准格式的退款DataFrame
        std_refund = pd.DataFrame()
        
        # 添加订单编号
        if '订单编号' in refund_mapping_result:
            std_refund['订单编号'] = extracted_refund[refund_mapping_result['订单编号']]
        else:
            print("警告: 无法在退款文件中找到订单编号列!")
            return intermediate_df  # 如果找不到订单编号，直接返回原始数据
        
        # 添加买家实际支付金额
        if '买家实际支付金额' in refund_mapping_result:
            std_refund['买家实际支付金额'] = pd.to_numeric(
                extracted_refund[refund_mapping_result['买家实际支付金额']], errors='coerce').fillna(0)
        else:
            # 如果没有支付金额列，设为0
            std_refund['买家实际支付金额'] = 0
        
        # 添加买家退款金额
        if '买家退款金额' in refund_mapping_result:
            std_refund['买家退款金额'] = pd.to_numeric(
                extracted_refund[refund_mapping_result['买家退款金额']], errors='coerce').fillna(0)
        else:
            print("警告: 无法在退款文件中找到退款金额列!")
            std_refund['买家退款金额'] = 0
        
        # 添加订单付款时间
        if '订单付款时间' in refund_mapping_result:
            std_refund['订单付款时间'] = extracted_refund[refund_mapping_result['订单付款时间']]
        else:
            # 如果没有付款时间，使用当前时间
            std_refund['订单付款时间'] = pd.Timestamp.now()
        
        # 替换原始退款DataFrame
        refund_df = std_refund
        
        # 1. 处理能匹配到订单的退款
        merged_df = pd.merge(intermediate_df, refund_df, on='订单编号', how='left', suffixes=('', '_refund'))
        print(f"合并后数据行数: {len(merged_df)}")
        
        # 更新买家实付和卖家实退字段
        merged_df['买家实付'] = merged_df.apply(
            lambda row: row['买家实际支付金额_refund'] if pd.notna(row.get('买家退款金额')) and row.get('买家退款金额', 0) > 0 else row['买家实际支付金额'], 
            axis=1
        )
        merged_df['卖家实退'] = merged_df['买家退款金额'].fillna(0)
        
        # 更新订单状态
        merged_df['订单状态'] = merged_df.apply(
            lambda row: '退款成功' if pd.notna(row.get('买家退款金额')) and row.get('买家退款金额', 0) > 0 else row['订单状态'], 
            axis=1
        )
        
        # 2. 找出未能匹配到订单的退款
        unmatched_refunds = refund_df[~refund_df['订单编号'].isin(intermediate_df['订单编号'])]
        print(f"未匹配退款数据行数: {len(unmatched_refunds)}")
        
        # 3. 为未匹配退款创建新记录
        if not unmatched_refunds.empty:
            unmatched_records = pd.DataFrame({
                '订单编号': unmatched_refunds['订单编号'],
                '支付单号': '',
                '买家实付': unmatched_refunds['买家实际支付金额'],
                '订单状态': '退款成功',
                '订单创建时间': unmatched_refunds['订单付款时间'],
                '商家备注': '跨月份退款订单',
                '卖家实退': unmatched_refunds['买家退款金额'],
                '手续费': 0,
                '确认收货时间': None,
                '打款商家金额': 0
            })
            
            # 4. 合并所有记录
            final_df = pd.concat([
                merged_df[['订单编号', '支付单号', '买家实付', '订单状态', '订单创建时间', 
                          '商家备注', '卖家实退', '手续费', '确认收货时间', '打款商家金额']], 
                unmatched_records
            ])
        else:
            final_df = merged_df[['订单编号', '支付单号', '买家实付', '订单状态', '订单创建时间', 
                                '商家备注', '卖家实退', '手续费', '确认收货时间', '打款商家金额']]
        
        print(f"最终数据行数: {len(final_df)}")
        return final_df
        
    except Exception as e:
        print(f"处理退款数据时出错: {str(e)}")
        import traceback
        print(f"错误堆栈: {traceback.format_exc()}")
        raise 