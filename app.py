import pandas as pd
import numpy as np
import streamlit as st
import warnings
import os
import gc
from datetime import datetime, timedelta, date
import io

# 设置页面配置
st.set_page_config(
    page_title="电商用户行为分析仪表",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)

warnings.filterwarnings('ignore')
pd.set_option('display.float_format', '{:.2f}'.format)

# 标题
st.title("🛒 电商用户行为分析仪表/ E-commerce User Behavior Analysis Dashboard")
st.markdown("---")

# 侧边栏配置
st.sidebar.markdown("### 📌 项目信息 / Project Info")

# 添加log1和log2（假设图片文件与app.py同目录）
col1, col2 = st.sidebar.columns(2)
with col1:
    st.sidebar.image("log1.png", use_column_width=True)  # 替换为实际log1图片路径
with col2:
    st.sidebar.image("log2.png", use_column_width=True)  # 替换为实际log2图片路径

# 个人和教授信息
st.sidebar.markdown("---")
st.sidebar.markdown("**个人信息 / Personal Info**")
st.sidebar.markdown("薛思怡 | siyi.xue@efrei.net")
st.sidebar.markdown("---")
st.sidebar.markdown("**教授信息 / Professor Info**")
st.sidebar.markdown("Mano Joseph Mathew | mano.mathew@efrei.fr")
st.sidebar.markdown("---")
st.sidebar.markdown("**GitHub 仓库 / GitHub Repository**")
st.sidebar.markdown("[https://github.com/xue414/userbehavior](https://github.com/xue414/userbehavior)")
st.sidebar.markdown("---")

# 原有配置参数部分
st.sidebar.header("⚙️ 配置参数 / Configuration Parameters")

# 文件上传 - 使用 Streamlit 的文件上传器
uploaded_file = st.sidebar.file_uploader(
    "上传数据 (UserBehavior.csv)", 
    type="csv",
    help="请上传包含用户行为数据的CSV文件"
)

# 抽样比例滑块
sample_ratio = st.sidebar.slider(
    "数据抽样比例 / Data Sampling Ratio", 
    min_value=0.001, 
    max_value=0.1, 
    value=0.01, 
    step=0.001,
    help="为了处理速度，建议使用较小的抽样比例"
)

# 数据加载函数（保持不变）
@st.cache_data
def load_and_preprocess_data(_uploaded_file, sample_fraction=0.02):
    """处理上传的数据文件"""
    
    if _uploaded_file is None:
        return pd.DataFrame()
    
    try:
        progress_text = st.empty()
        progress_bar = st.progress(0)
        
        progress_text.text("正在加载数据... / Loading data...")
        progress_bar.progress(20)
        
        # 读取上传的文件
        df = pd.read_csv(
            _uploaded_file,
            names=["user_id", "item_id", "category_id", "behavior_type", "timestamp"],
            header=None,
            low_memory=False
        )
        
        progress_bar.progress(40)
        progress_text.text("数据预处理中... / Data preprocessing...")
        
        # 抽样
        if sample_fraction < 1.0:
            df = df.sample(frac=sample_fraction, random_state=42)
        
        st.success(f"成功读取 {len(df):,} 行数据 / Successfully read {len(df):,} rows of data")
        
        # 检查行为类型的唯一性
        st.write(f"行为类型唯一性 / Unique behavior types: {df['behavior_type'].unique()}")
        
        # 时间戳转换
        df["datetime"] = pd.to_datetime(df["timestamp"], unit="s", errors="coerce")
        invalid_time_count = df["datetime"].isna().sum()
        if invalid_time_count > 0:
            st.warning(f"过滤无效时间戳 {invalid_time_count} 条 / Filtered invalid timestamps: {invalid_time_count} records")
            df = df.dropna(subset=["datetime"])
        
        if len(df) == 0:
            st.error("⚠️ 无有效时间数据 / No valid time data")
            return pd.DataFrame()
        
        # 衍生时间特征
        df["date"] = df["datetime"].dt.date
        df["hour"] = df["datetime"].dt.hour
        df["weekday"] = df["datetime"].dt.weekday + 1
        
        # 英文行为类型映射到中文
        behavior_map = {
            'pv': '浏览 / View',
            'fav': '收藏 / Favorite', 
            'cart': '加购 / Add to Cart',
            'buy': '购买 / Purchase'
        }
        
        df["behavior_name"] = df["behavior_type"].map(behavior_map)
        
        # 过滤无效行为类型
        invalid_behavior_count = df["behavior_name"].isna().sum()
        if invalid_behavior_count > 0:
            st.warning(f"过滤无效行为类型: {invalid_behavior_count} 条 / Filtered invalid behavior types: {invalid_behavior_count} records")
            df = df.dropna(subset=["behavior_name"])
        
        # 去重
        initial_count = len(df)
        df = df.drop_duplicates(subset=["user_id", "item_id", "behavior_type", "timestamp"])
        duplicate_count = initial_count - len(df)
        if duplicate_count > 0:
            st.info(f"去除重复记录: {duplicate_count} 条 / Removed duplicate records: {duplicate_count} records")
        
        progress_bar.progress(100)
        progress_text.text("数据预处理完成！ / Data preprocessing completed!")
        
        st.success(f"数据预处理完成：{len(df):,} 条有效记录 / Data preprocessing completed: {len(df):,} valid records")
        
        return df
        
    except Exception as e:
        st.error(f"数据加载失败：{str(e)} / Data loading failed: {str(e)}")
        return pd.DataFrame()

# 主应用逻辑（保持不变）
if uploaded_file is not None:
    # 加载数据
    df = load_and_preprocess_data(uploaded_file, sample_ratio)
    
    if not df.empty:
        # 固定日期范围
        start_date = df["date"].min()
        end_date = df["date"].max()
        
        # 固定行为类型
        selected_behaviors = ["浏览 / View", "收藏 / Favorite", "加购 / Add to Cart", "购买 / Purchase"]
        
        # 数据过滤
        df_filtered = df[
            (df["date"] >= start_date) & 
            (df["date"] <= end_date) & 
            (df["behavior_name"].isin(selected_behaviors))
        ].copy()
        
        st.info(f"📈 分析日期范围: {start_date} - {end_date} / Analysis date range: {start_date} - {end_date}")
        st.info(f"📊 过滤后数据量: {len(df_filtered):,} 条记录 / Filtered data volume: {len(df_filtered):,} records")
        
        # 数据分析
        st.header("📊 数据分析结果 / Data Analysis Results")
        
        # 使用列布局显示基础统计
        col1, col2, col3 = st.columns(3)
        
        with col1:
            total_users = df_filtered["user_id"].nunique()
            st.metric("总用户数 / Total Users", f"{total_users:,}")
        
        with col2:
            total_items = df_filtered["item_id"].nunique()
            st.metric("总商品数 / Total Items", f"{total_items:,}")
        
        with col3:
            total_behaviors = len(df_filtered)
            st.metric("总行为数 / Total Behaviors", f"{total_behaviors:,}")
        
        # 行为分布
        behavior_dist = df_filtered["behavior_name"].value_counts()
        
        st.subheader("📊 行为类型分布 / Behavior Type Distribution")
        behavior_cols = st.columns(4)
        
        for idx, (behavior, count) in enumerate(behavior_dist.items()):
            percentage = (count / total_behaviors) * 100
            with behavior_cols[idx]:
                st.metric(behavior, f"{count:,}", f"{percentage:.1f}%")
        
        # 转化率计算
        if "浏览 / View" in behavior_dist and "购买 / Purchase" in behavior_dist:
            purchase_rate = (behavior_dist["购买 / Purchase"] / behavior_dist["浏览 / View"]) * 100
            st.success(f"📈 浏览→购买转化率: {purchase_rate:.2f}% / View→Purchase conversion rate: {purchase_rate:.2f}%")
        else:
            purchase_rate = 0
            st.warning("⚠️ 无法计算转化率：缺少浏览或购买数据 / Cannot calculate conversion rate: missing view or purchase data")
        
        # 时间分析
        st.header("⏰ 时间模式分析 / Time Pattern Analysis")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # 小时分布
            hourly_behavior = df_filtered.groupby("hour").size().reset_index(name="behavior_count")
            peak_hour = hourly_behavior.loc[hourly_behavior['behavior_count'].idxmax(), 'hour']
            
            st.subheader("用户活跃小时分布 / User Activity Hourly Distribution")
            st.bar_chart(hourly_behavior.set_index("hour"))
            st.info(f"活跃高峰时段: {peak_hour}:00 / Peak activity hour: {peak_hour}:00")
        
        with col2:
            # 周内分布
            weekday_behavior = df_filtered.groupby("weekday").size().reset_index(name="behavior_count")
            weekday_map = {1:"周一/Mon",2:"周二/Tue",3:"周三/Wed",4:"周四/Thu",5:"周五/Fri",6:"周六/Sat",7:"周日/Sun"}
            weekday_behavior["weekday_name"] = weekday_behavior["weekday"].map(weekday_map)
            peak_weekday = weekday_behavior.loc[weekday_behavior['behavior_count'].idxmax(), 'weekday_name']
            
            st.subheader("周内活跃分布 / Weekly Activity Distribution")
            st.bar_chart(weekday_behavior.set_index("weekday_name"))
            st.info(f"最活跃的星期: {peak_weekday} / Most active weekday: {peak_weekday}")
        
        # 用户分层分析
        st.header("👥 用户分层分析 / User Segmentation Analysis")
        
        user_behavior_count = df_filtered.groupby("user_id").size().reset_index(name="total_behavior")
        user_segments = pd.cut(
            user_behavior_count["total_behavior"],
            bins=[0, 5, 20, 100, float("inf")],
            labels=["低活跃(1-5次)", "中活跃(6-20次)", "高活跃(21-100次)", "超高活跃(100+次)"]
        )
        segment_dist = user_segments.value_counts()
        
        # 显示用户分层统计
        st.subheader("用户活跃度分布 / User Activity Distribution")
        segment_cols = st.columns(4)
        
        for idx, (segment, count) in enumerate(segment_dist.items()):
            percentage = (count / len(user_behavior_count)) * 100
            with segment_cols[idx]:
                st.metric(segment, f"{count:,}", f"{percentage:.1f}%")
        
        # 热门商品和品类
        st.header("📦 商品与品类分析 / Item and Category Analysis")
        
        top_items = df_filtered.groupby("item_id").size().nlargest(5).reset_index(name="behavior_count")
        top_categories = df_filtered.groupby("category_id").size().nlargest(5).reset_index(name="behavior_count")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("最热门商品 / Most Popular Items")
            for i, row in top_items.iterrows():
                st.write(f"{i+1}. 商品ID {row['item_id']}: {row['behavior_count']} 次行为")
        
        with col2:
            st.subheader("最热门品类 / Most Popular Categories")
            for i, row in top_categories.iterrows():
                st.write(f"{i+1}. 品类ID {row['category_id']}: {row['behavior_count']} 次行为")
        
        # 核心洞察
        st.header("💡 核心洞察与运营启示 / Key Insights and Operational Implications")
        
        high_active_percentage = (segment_dist.get("高活跃(21-100次)", 0) + 
                                 segment_dist.get("超高活跃(100+次)", 0)) / len(user_behavior_count) * 100
        
        insights = [
            "### 📊 核心洞察 / Key Insights",
            f"- **时间洞察**：用户活跃高峰集中在{peak_hour}:00-{peak_hour+2}:00时段，{peak_weekday}最为活跃",
            f"- **Time Insight**: Peak user activity concentrated in {peak_hour}:00-{peak_hour+2}:00, most active on {peak_weekday}",
            f"- **转化洞察**：浏览→购买转化率 {purchase_rate:.2f}%",
            f"- **Conversion Insight**: View→Purchase conversion rate: {purchase_rate:.2f}%",
            f"- **用户洞察**：{high_active_percentage:.1f}%的高活跃用户贡献主要行为",
            f"- **User Insight**: {high_active_percentage:.1f}% high-active users contribute most behaviors",
            f"- **商品洞察**：最热门商品ID {top_items.iloc[0]['item_id']} ({top_items.iloc[0]['behavior_count']} 次行为)",
            f"- **Item Insight**: Most popular item ID {top_items.iloc[0]['item_id']} ({top_items.iloc[0]['behavior_count']} behaviors)",
            "",
            "### 🎯 运营启示 / Operational Implications",
            f"- 🕒 **优化营销时机**：在{peak_hour}:00-{peak_hour+2}:00高峰时段和{peak_weekday}加强营销活动",
            f"- 🕒 **Optimize Timing**: Strengthen marketing during peak hours {peak_hour}:00-{peak_hour+2}:00 and on {peak_weekday}",
            f"- 💎 **用户分层运营**：针对{high_active_percentage:.1f}%高价值用户设计专属权益",
            f"- 💎 **User Segmentation**: Design exclusive benefits for {high_active_percentage:.1f}% high-value users",
            f"- 🎯 **提升转化率*：重点关注浏览→购买转化路径优化",
            f"- 🎯 **Improve Conversion**: Focus on optimizing view→purchase conversion path"
        ]
        
        for insight in insights:
            if insight.startswith("###"):
                st.markdown(insight)
            elif insight.startswith("- **"):
                st.markdown(insight)
            else:
                st.write(insight)
        
        # 数据样本预览
        with st.expander("📋 数据样本预览 / Data Sample Preview"):
            st.dataframe(df_filtered.head(100))
        
        # 最终内存清理
        gc.collect()
        
    else:
        st.error("数据加载失败，请检查文件格式 / Data loading failed, please check file format")

else:
    # 显示使用说明
    st.markdown("""
    ## 📋 使用说明 / Instructions
    
    1. **上传数据**：在左侧边栏上传 `UserBehavior.csv` 文件
       - **Upload Data**: Upload `UserBehavior.csv` file in the left sidebar
       
    2. **调整参数**：使用滑块调整数据抽样比例
       - **Adjust Parameters**: Use the slider to adjust data sampling ratio
       
    3. **查看分析**：系统将自动生成完整的用户行为分析报告
       - **View Analysis**: The system will automatically generate a complete user behavior analysis report
    
    ### 💡 大文件处理提示 / Large File Handling Tips
    - 对于超过200MB的大文件，建议使用较小的抽样比例（如0.001-0.01）
    - For files larger than 200MB, use smaller sampling ratios (e.g., 0.001-0.01)
    - Streamlit Cloud 支持最大200MB的文件上传
    - Streamlit Cloud supports file uploads up to 200MB
    
    ### 📊 数据格式说明 / Data Format Description
    - `user_id`: 用户ID / User ID
    - `item_id`: 商品ID / Item ID  
    - `category_id`: 品类ID / Category ID
    - `behavior_type`: 行为类型 (pv=浏览, fav=收藏, cart=加购, buy=购买) / Behavior Type
    - `timestamp`: 时间戳 / Timestamp
    """)

# 页脚
st.markdown("---")
st.markdown(
    "🛒 **电商用户行为分析仪表盘** | "
    "**E-commerce User Behavior Analysis Dashboard** | "
    "基于 Streamlit 构建 / Built with Streamlit"
)