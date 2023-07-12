import plotly.express as px
import streamlit as st
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import desc, split
import matplotlib.pyplot as plt
import plotly.express as px
# from pyspark.sql.functions import lit
import functools
# from pyspark.sql import DataFrame
import plotly.graph_objects as go
import pandas as pd
import pickle
from plotly.subplots import make_subplots



st.set_page_config(
	page_title="An Analysis of Transactional Messages", layout="wide")

st.markdown("<h1 style='text-align: center'> An Analysis of 10M+ Transactional Messages </h1>", unsafe_allow_html=True)

@st.cache_data
def get_dfs():
	campaigns_df = pd.read_csv("data/campaigns.csv")
	holidays_df = pd.read_csv("data/holidays.csv")
	return campaigns_df, holidays_df

def get_frequency():
	campaigns_df, holidays_df = get_dfs()
	holidays_df["date"] = pd.to_datetime(holidays_df["date"]).dt.date
	campaigns_df["started_at"] = pd.to_datetime(campaigns_df["started_at"]).dt.date

	temp_df = campaigns_df.groupby(["started_at", "campaign_type"])[["id"]].count().reset_index().rename({"id":"count"}, axis=1)

	fig_1 = px.area(temp_df, x="started_at", y="count", color="campaign_type")

	with open("dfs/other_msgs.txt", "rb") as f:
		other_msgs_df = pickle.load(f)

	fig_2 = px.area(other_msgs_df, x="sent_at", y="count", color="message_type")

	return fig_1, fig_2

def get_subject_pies():
	with open("dfs/subject_pies_missing.txt", "rb") as f:
		final_dfs = pickle.load(f)

	with open("dfs/subject_pies.txt", "rb") as f:
		final_dfs_2 = pickle.load(f) 

	combined_dfs = []

	for i in range(4):
		combined_dfs.append(final_dfs[i])
		combined_dfs.append(final_dfs_2[i])
		
	fig = make_subplots(rows=2, cols=4, specs=[[{"type": "pie"}, {"type": "pie"}, {"type": "pie"}, {"type": "pie"}],
	 [{"type": "pie"}, {"type": "pie"}, {"type": "pie"}, {"type": "pie"}]], subplot_titles=("Without Personalization", "With Personalization", "Without Discount", "With Discount", "Without Saleout", "With Saleout", "Without Deadline", "With Deadline"))

	col = 1
	row = 1
	for i in range(8):
		fig.add_trace(go.Pie(
			values=combined_dfs[i]["Count"],
			labels=combined_dfs[i]["Type"], hole=0.5, 
			domain=dict(x=[0, 0.5])), 
			row=row, col=col)
		col += 1
		
		if col == 5:
			col = 1
			row += 1  


	fig.update_layout(height=600, width=1600, showlegend=False)

	return fig

def get_open_rate_bars():
	with open("dfs/open_rate_bars.txt", "rb") as f:
		chart_2_df = pickle.load(f)	
	fig = px.bar(chart_2_df, x="topic", y="open_rate", color="message_type")
	fig.update_layout(xaxis={"categoryorder":"total descending"})
	return fig

def get_sunburst():
	with open("dfs/open_rate_bars.txt", "rb") as f:
		chart_2_df = pickle.load(f)	
	fig = px.sunburst(chart_2_df, path=["message_type", "channel", "topic"], color="message_type", values="total")
	fig.update_layout(height=700, width=700)

	return fig

def get_line():
	with open("dfs/purch_atp.txt", "rb") as f:
		purch_atp_df = pickle.load(f)

	purch_atp_df["percent_purchases"] = purch_atp_df["percent_purchases"] * 100
	purch_atp_df.loc[len(purch_atp_df.index)] = ["ideal value", 100, 0]	
	fig = px.scatter(purch_atp_df, x="avg_time_to_purchase", y="percent_purchases", color="message_type")
	# fig.update_layout(height=700, width=700)

	return fig

def get_line_2():
	with open("dfs/unsub_blocked.txt", "rb") as f:
		unsub_blocked_df = pickle.load(f)

	unsub_blocked_df.loc[len(unsub_blocked_df.index)] = ["ideal value", 0, 0]	
	fig = px.scatter(unsub_blocked_df, x="percent_unsubscribed", y="percent_blocked", color="message_type")
	# fig.update_layout(height=700, width=700)

	return fig



def main():
	st.write("Let's see if messages sent out by companies affect the behavior of users.")

	st.write("First, let's see how these messages are broken down and how frequently they're sent.")
	fig_1, fig_2 = get_frequency()
	st.plotly_chart(get_sunburst(), use_container_width=True)
	st.plotly_chart(fig_1, use_container_width=True)
	st.plotly_chart(fig_2, use_container_width=True)


	st.write("Next, let's see which messages are being opened.")
	st.plotly_chart(get_open_rate_bars(), use_container_width=True)

	st.write("Looks like most messages being opened are transactional ones. \
		However, bulk messages are more directly related to the effects of marketing. Do certain keywords in subjects improve the open rate?")
	sub_col1, sub_col2 = st.columns(2)
	st.plotly_chart(get_subject_pies(), use_container_width=True)

	# st.write("How often do messages lead to purchases?")

	st.write("Which types of messages lead to the most purchases? \n")
	st.plotly_chart(get_line(), use_container_width=True)

	st.write("Which types of messages lead to some adverse action?")
	st.plotly_chart(get_line_2(), use_container_width=True)




	return None

if __name__ == "__main__":
	main()
