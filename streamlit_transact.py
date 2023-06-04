import plotly.express as px
import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, split
import matplotlib.pyplot as plt
import plotly.express as px
from pyspark.sql.functions import lit
import functools
from pyspark.sql import DataFrame
import plotly.graph_objects as go
import pandas as pd
import pickle

st.set_page_config(
	page_title="An Analysis of 100 million+ eCommerce Transactions", layout="wide")

st.markdown("<h1 style='text-align: center'> An Analysis of 100 million+ eCommerce Transactions </h1>", unsafe_allow_html=True)

css='''
	[data-testid="metric-container"] {
	    width: fit-content;
	    margin: auto;
	}

	[data-testid="metric-container"] > div {
	    width: fit-content;
	    margin: auto;
	}

	[data-testid="metric-container"] label {
	    width: fit-content;
	    margin: auto;
	}
	'''


@st.cache_data
def get_agg_by_brand():
	count_by_brand = pd.read_csv("agg_by_brand.csv").dropna().iloc[:14]

	country = ["Korea", "US", "China", "China", "Canada", "Italy", "China", "Korea", "Japan",
			   "Uzbekistan", "Germany", "Taiwan", "China", "Finland"]
	count_by_brand["country"] = country
	# count_by_brand = count_by_brand.set_index("brand")
	print(list(count_by_brand["brand"]))
	return count_by_brand

def get_agg_by_brand_plot(agg_by_brand):
	fig = px.scatter(agg_by_brand,  y="count", color="country",
					 size="revenue", x="brand", title="Top 15 Brands Based on Purchases", category_orders=dict(group=[agg_by_brand["brand"]]))
	# fig.update_layout(width=750,height=750)

	return fig

@st.cache_data
def get_day_stats():
	day_stats = pd.read_csv("day_stats.csv")

	return day_stats

def get_day_plots(day_stats):
	fig_1 = px.bar(day_stats, y="day", x=["oct_count", "nov_count"], orientation='h', title=" ")
	fig_1.update_layout(width=750, height=750, xaxis_title="Number of Purchases")

	fig_2 = px.bar(day_stats, y="day", x=["oct_rev", "nov_rev"], orientation='h', title="Tracking Purchases Throughout the Month", color_discrete_map={
        'oct_count': 'red',
        'nov_count': 'green'
    })
	fig_2.update_layout(xaxis_title="Total Revenue", xaxis=dict(autorange="reversed"), width=750, height=750, font_color="white")
	fig_2.update_yaxes(visible=False)

	return fig_1, fig_2

@st.cache_data
def get_brand_counts():
	brand_counts = pd.read_csv("brand_counts.csv")

	return brand_counts

def get_brand_counts_plot(brand_counts):
	fig = px.pie(brand_counts, values='count', names='adjusted_brand', hole=0.3, title="Brand Totals")

	return fig

@st.cache_data
def get_vtp():
	vtp = pd.read_csv("v_times.csv")

	return vtp

def get_vtp_plot(vtp):
	fig = px.scatter(vtp, y=["avg_vtp_time", "avg_vtc_time"], x="brand", title="User Behavior Analysis")	

	return fig

def get_sankey():
	sankey_df = pd.read_csv("sankey.csv")
	with open("sankey_labels.txt", "rb") as f:
		labels = pickle.load(f)

	source = sankey_df["main_cat_index"] 
	target = sankey_df["subcat_1_index_adjusted"]
	value = sankey_df["count"]
	# data to dict, dict to sankey
	link = dict(source = source, target = target, value = value)
	node = dict(label = labels, pad=50, thickness=5)
	data = go.Sankey(link = link, node = node)
	# plot
	fig = go.Figure(data)
	fig.update_layout(
	    width=750,
	    height=1000,
	    title="Breakdown of Product Categories (Purchases)",

	)

	return fig

def main():
	# spark = SparkSession.builder.appName('test').getOrCreate()
	# df = process_dfs(spark, ["data/2019-Nov.csv"])
	# print(df.count())
	col1, col2, col3, col4, col5 = st.columns(5)
	col3.metric("Dataset Information", "")
	col1, col2, col3, col4 = st.columns(4)
	col1.metric("Time Range", "2 Months")
	col2.metric("Total Transactions", "110M")
	col3.metric("Total Brands", "4304")
	col4.metric("Total Product Categories", "14")
	col1, col2, col3, col4, col5 = st.columns(5)

	col3.metric("Quick Facts", "")
	col1, col2, col3, col4 = st.columns(4)
	col4.metric("Lowest View-to-Purchase Time", "Apple")
	col1.metric("Most Popular Category", "Electronics")
	col2.metric("Most Popular Brand (Revenue)", "Apple")
	col3.metric("Most Popular Brand (Purchases)", "Samsung")
	st.markdown(f'<style>{css}</style>',unsafe_allow_html=True)

	col_1, col_2, col_3 = st.columns((2, 5, 2), gap="small")
	
	temp_df = get_brand_counts()

	plot = get_sankey()
	col_2.plotly_chart(plot, use_container_width=True)

	col_1, col_2 = st.columns(2)
	plot = get_brand_counts_plot(temp_df)
	col_1.plotly_chart(plot, use_container_width=True)
	temp_df = get_agg_by_brand()
	plot = get_agg_by_brand_plot(temp_df)
	col_2.plotly_chart(plot, use_container_width=True)

	temp_df_2 = get_day_stats()
	count_plot, rev_plot = get_day_plots(temp_df_2)
	col_1, col_2 = st.columns(2, gap="small")
	col_1.plotly_chart(rev_plot, use_container_width=True)
	col_2.plotly_chart(count_plot, use_container_width=True)


	col11, col22 = st.columns((2, 7), gap="large")
	vtp = get_vtp()
	plot = get_vtp_plot(vtp)
	col11.markdown("**View to Purchase Time:** The amount of time between a user's viewing of an item to the user's purchase. \
	\n\n **View to Cart Time:** The amount of time between a user's viewing of an item to the user adding the item to cart.\
	 \n\n I could only perform this SQL query on 34 million transactions before my ram was used up :/ \
	 \n\n Therefore I only looked at the top 5 categories.")
	col22.plotly_chart(plot, use_container_width=True)

if __name__ == "__main__":
	main()