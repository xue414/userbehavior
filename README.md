# 电商用户行为分析仪表盘

E-commerce User Behavior Analysis Dashboard

## 功能特性

- 📊 用户行为数据分析
- ⏰ 时间模式分析  
- 👥 用户分层分析
- 📈 转化漏斗分析
- 🎯 运营洞察建议

## 部署到 Streamlit Cloud

1. Fork 这个仓库
2. 访问 [Streamlit Cloud](https://streamlit.io/cloud)
3. 点击 "New app"
4. 选择你的仓库、分支和主文件路径 (`app.py`)
5. 点击 "Deploy"

## 数据格式

CSV文件应包含以下列（无表头）：
- user_id
- item_id
- category_id
- behavior_type (pv, fav, cart, buy)
- timestamp
