import streamlit as st
import pandas as pd
import plotly.express as px

st.title("Storage Format Benchmark Dashboard")

df = pd.read_csv("results/benchmark_results.csv")

st.subheader("Benchmark Results")
st.dataframe(df)

fig1 = px.bar(df, x="Format", y="Storage_MB", title="Storage Size (MB)", color="Format")
st.plotly_chart(fig1)

fig2 = px.bar(df, x="Format", y=["Filter_Time","Aggregation_Time"], barmode="group", title="Query Performance")
st.plotly_chart(fig2)

st.subheader("Alerts")

alerts = []
for _, r in df.iterrows():
    if r["Filter_Time"] > 3:
        alerts.append(f"⚠️ Slow filter query detected: {r['Format']}")
    if r["Storage_MB"] > 20:
        alerts.append(f"⚠️ Large storage usage detected: {r['Format']}")

if alerts:
    for a in alerts:
        st.warning(a)
else:
    st.success("System performance normal")