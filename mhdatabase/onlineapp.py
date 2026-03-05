import streamlit as st
import sqlite3
import pandas as pd
import pydeck as pdk
import requests
import os
from zhipuai import ZhipuAI

# ---------- 配置 ----------
# ！！！重要：请修改为你的数据库文件绝对路径 ！！！
DB_PATH = "hubei_epidemic.db"  # 改成你的实际路径

# 本地Qwen模型的API地址（通过Ollama运行）
OLLAMA_URL = "http://localhost:11434/api/generate"


# ---------- 获取数据库表结构 ----------
def get_schema_info():
    """从数据库读取表结构，返回字符串描述"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    schema = ""
    for table in tables:
        table_name = table[0]
        cursor.execute(f"PRAGMA table_info({table_name});")
        columns = cursor.fetchall()
        col_names = [col[1] for col in columns]
        col_types = [col[2] for col in columns]
        schema += f"表名：{table_name}\n字段：{', '.join([f'{n} ({t})' for n, t in zip(col_names, col_types)])}\n"
    conn.close()
    # 补充关联关系说明
    schema += "\n关联关系：\n"
    schema += "event_table.location_id 关联到 location_table.id\n"
    schema += "注意：经度字段（longitude）存储格式如 '114.87°E'，纬度字段（latitude）存储格式如 '30.45°N'，查询时需要用 REPLACE 函数去掉 '°E' 和 '°N' 并转换为数字。\n"
    return schema


# ---------- 调用智谱AI模型 ----------
def call_llm(prompt):
    """调用智谱AI API，返回模型生成的文本"""
    # 从环境变量读取API密钥（部署到Streamlit Cloud时会设置）
    import os
    api_key = "df6dd6e22d85486f9131d5048ea4dcf6.dr9CRHieJDPi1t9b"
    if not api_key:
        return "错误：未设置API密钥"

    client = ZhipuAI(api_key=api_key)
    try:
        response = client.chat.completions.create(
            model="glm-4-flash",  # 使用快速便宜的模型
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=1024
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"调用智谱AI失败：{str(e)}"


# ---------- 自然语言转SQL ----------
def text_to_sql(question, schema_info):
    prompt = f"""
你是一个SQLite专家。请根据以下数据库结构，将用户的问题转换为SQLite查询语句。

数据库结构：
{schema_info}

**重要限制**：
1. **只能使用以下两个表**：`event_table` 和 `location_table`。绝对禁止使用任何其他表。
2. **只能使用表中实际存在的字段**：
   - `event_table` 的字段：`id`, `dynasty`, `year`, `location_id`, `epidemics`, `field6`, `field7`
   - `location_table` 的字段：`id`, `historical_name`, `modern_name`, `longitude`, `latitude`
3. 关联条件固定为：`event_table.location_id = location_table.id`
4. 所有地点都在湖北省，**不需要加省份条件**。
5. 经纬度字段是文本（如 `114.87°E`），若要显示数字，请用：
   `REPLACE(longitude, '°E', '')` 和 `REPLACE(latitude, '°N', '')`，并可用 `CAST(... AS REAL)` 转为实数。
6. **年份处理**：`event_table` 中的 `year` 字段是 `TEXT` 类型，可能包含空格或特殊字符。为了准确匹配，请在查询条件中使用 `TRIM(year) = '具体年份'`。
7. 如果问题涉及朝代（如“清代”、“光绪年间”），请根据年份范围筛选，具体映射如下：
   - 明代：1368-1644
   - 清代：1644-1911
   - 顺治：1644-1661
   - 康熙：1662-1722
   - 雍正：1723-1735
   - 乾隆：1736-1795
   - 嘉庆：1796-1820
   - 道光：1821-1850
   - 咸丰：1851-1861
   - 同治：1862-1874
   - 光绪：1875-1908
   - 宣统：1909-1911
   - 民国：1912-1949
8. 问题中的“疫灾”已经隐含在事件表中，**不需要额外条件**（例如不要加 `epidemics = '1'` 这类条件，因为事件表只记录疫灾）。

**输出要求**：
- 只输出SQL语句，不要任何解释、注释、Markdown格式。
- 如果用户问题无法用现有表回答，输出空字符串。

**示例**：
- 用户问：“1808年湖北哪些县有疫灾？”  
  正确SQL：
  ```sql
  SELECT l.historical_name, 
         REPLACE(l.longitude, '°E', '') AS longitude, 
         REPLACE(l.latitude, '°N', '') AS latitude
  FROM event_table e
  JOIN location_table l ON e.location_id = l.id
  WHERE TRIM(e.year) = '1808';
- 用户问：“清代光绪年间的疫灾记录”
  正确SQL：
  SELECT e.year, l.historical_name
  FROM event_table e
  JOIN location_table l ON e.location_id = l.id
  WHERE e.year BETWEEN 1875 AND 1908;

现在请处理用户问题：{question}

SQL语句：
    """
    # 调用LLM生成SQL
    sql = call_llm(prompt)
    # 清理可能的代码块标记
    sql = sql.strip()
    if sql.startswith("```sql"):
        sql = sql.replace("```sql", "").replace("```", "").strip()
    elif sql.startswith("sql"):
        sql = sql.replace("sql", "").strip()
    elif "```" in sql:
        sql = sql.replace("```", "").strip()
    return sql


# ---------- 执行SQL ----------
def execute_sql(sql):
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(sql, conn)
        return df
    except Exception as e:
        st.error(f"SQL执行错误：{e}\n生成的SQL：{sql}")
        return None
    finally:
        conn.close()


# ---------- 清洗经纬度 ----------
def clean_coordinates(df):
    df_clean = df.copy()
    for col in df_clean.columns:
        col_lower = col.lower()
        if 'longitude' in col_lower or 'lon' in col_lower or 'lng' in col_lower:
            df_clean[col] = df_clean[col].astype(str).str.replace('°E', '', regex=False)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        elif 'latitude' in col_lower or 'lat' in col_lower:
            df_clean[col] = df_clean[col].astype(str).str.replace('°N', '', regex=False)
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
    return df_clean


# ---------- Streamlit界面 ----------
st.set_page_config(page_title="湖北历史疫灾智能查询", layout="wide")
# ---------- 调试信息：检查文件和数据库 ----------
st.write("当前工作目录:", os.getcwd())
st.write("目录下文件列表:", os.listdir('.'))

try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    st.write("数据库中的表:", tables)
    conn.close()
except Exception as e:
    st.error(f"数据库连接失败: {e}")
st.title("🦠 湖北历史疫灾智能查询系统")
st.markdown("输入自然语言问题，例如：“1808年湖北哪些县有疫灾？” 或 “清代光绪年间的疫灾记录”")

with st.sidebar:
    st.header("数据库结构")
    try:
        schema_info = get_schema_info()
        st.text(schema_info)
    except Exception as e:
        st.error(f"无法读取数据库：{e}")

question = st.text_input("请输入你的问题：", key="question")

if question:
    with st.spinner("正在理解问题并生成SQL..."):
        try:
            schema = get_schema_info()
        except Exception as e:
            st.error(f"数据库连接失败：{e}")
            st.stop()

        sql = text_to_sql(question, schema)
        st.code(sql, language="sql")

        df = execute_sql(sql)

        if df is not None and not df.empty:
            st.success(f"查询到 {len(df)} 条记录")
            df_clean = clean_coordinates(df)
            st.subheader("查询结果")
            st.dataframe(df_clean)

            # 找出经纬度列
            lat_col = None
            lon_col = None
            for col in df_clean.columns:
                col_lower = col.lower()
                if 'lat' in col_lower:
                    lat_col = col
                if 'lon' in col_lower or 'lng' in col_lower:
                    lon_col = col

            if lat_col and lon_col:
                map_df = df_clean[[lat_col, lon_col]].dropna()
                if not map_df.empty:
                    st.subheader("疫灾分布地图")
                    view_state = pdk.ViewState(
                        latitude=map_df[lat_col].mean(),
                        longitude=map_df[lon_col].mean(),
                        zoom=6,
                        pitch=0
                    )
                    layer = pdk.Layer(
                        'ScatterplotLayer',
                        data=map_df,
                        get_position=[lon_col, lat_col],
                        get_radius=10000,
                        get_fill_color=[255, 0, 0, 160],
                        pickable=True
                    )

                    # 获取地名列用于提示
                    name_col = None
                    for col in df_clean.columns:
                        if 'name' in col.lower() or 'historical' in col.lower():
                            name_col = col
                            break
                    if name_col:
                        tooltip = {"text": f"{name_col}: {{{name_col}}}\n经度: {{{lon_col}}}\n纬度: {{{lat_col}}}"}
                    else:
                        tooltip = {"text": "点击查看详情"}
                    r = pdk.Deck(
                        layers=[layer],
                        initial_view_state=view_state,
                        tooltip=tooltip
                    )
                    st.pydeck_chart(r)
                else:
                    st.info("地图数据缺失有效坐标")
            else:
                st.info("查询结果中不包含经纬度信息，无法显示地图")
        elif df is not None:
            st.warning("查询结果为空，没有找到匹配的数据。")
