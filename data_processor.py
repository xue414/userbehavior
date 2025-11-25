import pandas as pd
import numpy as np
import argparse
import os
import sys

def create_sample_data(input_file=None, output_file="UserBehavior_sampled.csv", sample_ratio=0.01):
    """
    用户行为数据预处理工具
    """
    
    if input_file and os.path.exists(input_file):
        print(f"🔍 正在从 {input_file} 抽样 {sample_ratio*100}% 数据...")
        
        try:
            # 直接读取抽样
            df = pd.read_csv(
                input_file,
                names=["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
                header=None,
                low_memory=False
            )
            
            print(f"📊 原始数据行数: {len(df):,}")
            
            # 抽样
            sampled_df = df.sample(frac=sample_ratio, random_state=42)
            sampled_df.to_csv(output_file, index=False, header=False)
            
            print(f"✅ 抽样完成！")
            print(f"📁 输出文件: {output_file}")
            print(f"📈 抽样数据: {len(sampled_df):,} 行")
            
        except MemoryError:
            # 分块读取
            print("⚠️  内存不足，使用分块读取...")
            process_large_file_in_chunks(input_file, output_file, sample_ratio)
            
    else:
        # 生成示例数据
        print("🎲 正在生成示例数据...")
        generate_demo_data(output_file)

def process_large_file_in_chunks(input_file, output_file, sample_ratio, chunksize=100000):
    """分块处理大文件"""
    chunks = []
    total_rows = 0
    
    for chunk_num, chunk in enumerate(pd.read_csv(
        input_file,
        names=["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
        header=None,
        chunksize=chunksize,
        low_memory=False
    )):
        sampled_chunk = chunk.sample(frac=sample_ratio, random_state=42)
        chunks.append(sampled_chunk)
        total_rows += len(chunk)
        
        if chunk_num % 10 == 0:
            print(f"📦 已处理: {total_rows:,} 行")
    
    result = pd.concat(chunks, ignore_index=True)
    result.to_csv(output_file, index=False, header=False)
    
    print(f"✅ 处理完成！")
    print(f"📁 输出文件: {output_file}")
    print(f"📈 总行数: {len(result):,}")

def generate_demo_data(output_file, n_samples=50000):
    """生成演示数据"""
    np.random.seed(42)
    
    demo_data = pd.DataFrame({
        'user_id': np.random.randint(1, 1000, n_samples),
        'item_id': np.random.randint(1, 5000, n_samples),
        'category_id': np.random.randint(1, 100, n_samples),
        'behavior_type': np.random.choice(['pv', 'fav', 'cart', 'buy'], n_samples, p=[0.7, 0.1, 0.15, 0.05]),
        'timestamp': np.random.randint(1577808000, 1577894400, n_samples)
    })
    
    demo_data.to_csv(output_file, index=False, header=False)
    print(f"✅ 示例数据已生成: {output_file}")
    print(f"📊 数据行数: {len(demo_data):,}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='用户行为数据预处理工具')
    parser.add_argument('--input', help='输入文件路径（可选）', default=None)
    parser.add_argument('--output', default='UserBehavior_sampled.csv', help='输出文件路径')
    parser.add_argument('--ratio', type=float, default=0.01, help='抽样比例 (0.01 = 1%%)')
    
    args = parser.parse_args()
    
    print("=" * 50)
    print("🛒 电商用户行为数据预处理工具")
    print("=" * 50)
    
    create_sample_data(args.input, args.output, args.ratio)