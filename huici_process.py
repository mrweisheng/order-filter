import pandas as pd
import os

def process_orders(order_df):
    """处理订单数据"""
    try:
        print(f"原始订单数据行数: {len(order_df)}")
        
        # 提取订单文件中的必要字段
        order_df = order_df[['订单编号', '支付单号', '买家实际支付金额', '订单状态', '订单创建时间', 
                            '商家备注', '确认收货时间', '打款商家金额', '卖家服务费', '买家服务费']]
        
        print(f"提取字段后订单数据行数: {len(order_df)}")
        
        # 确保数值字段为数值类型，并处理缺失值
        numeric_cols = ['买家实际支付金额', '卖家服务费', '买家服务费']
        for col in numeric_cols:
            order_df[col] = pd.to_numeric(order_df[col], errors='coerce')
            # 打印每个字段的非空值数量
            print(f"{col} 非空值数量: {order_df[col].notna().sum()}")
        
        # 计算手续费
        order_df['手续费'] = order_df['卖家服务费'].fillna(0) + order_df['买家服务费'].fillna(0)
        
        # 选择最终需要的字段
        intermediate_df = order_df[['订单编号', '支付单号', '买家实际支付金额', '订单状态', 
                                  '订单创建时间', '商家备注', '确认收货时间', '打款商家金额', '手续费']]
        
        print(f"最终处理后订单数据行数: {len(intermediate_df)}")
        return intermediate_df
        
    except Exception as e:
        print(f"处理订单数据时出错: {str(e)}")
        raise

def update_with_refunds(intermediate_df, refund_df):
    """处理退款数据"""
    try:
        print(f"订单数据行数: {len(intermediate_df)}")
        print(f"退款数据行数: {len(refund_df)}")
        
        # 提取退款文件中的必要字段
        refund_df = refund_df[['订单编号', '买家实际支付金额', '买家退款金额', '订单付款时间']]
        
        # 确保数值字段为数值类型，并处理缺失值
        numeric_cols = ['买家实际支付金额', '买家退款金额']
        for col in numeric_cols:
            refund_df[col] = pd.to_numeric(refund_df[col], errors='coerce')
        
        # 1. 处理能匹配到订单的退款
        merged_df = pd.merge(intermediate_df, refund_df, on='订单编号', how='left', suffixes=('', '_refund'))
        print(f"合并后数据行数: {len(merged_df)}")
        
        # 更新买家实付和卖家实退字段
        merged_df['买家实付'] = merged_df.apply(
            lambda row: row['买家实际支付金额_refund'] if pd.notna(row['买家退款金额']) else row['买家实际支付金额'], 
            axis=1
        )
        merged_df['卖家实退'] = merged_df['买家退款金额'].fillna(0)
        
        # 更新订单状态
        merged_df['订单状态'] = merged_df.apply(
            lambda row: '退款成功' if pd.notna(row['买家退款金额']) else row['订单状态'], 
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
        raise

def main(order_file, refund_file):
    """主处理函数 - 不再使用"""
    pass 