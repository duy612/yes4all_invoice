import pandas as pd
import streamlit as st
import plotly.express as pe
import xlrd
import openpyxl
import datetime

st.set_page_config(page_title = 'Yes4All_Invoice',
                    page_icon = ":money_with_wings:",
                    layout = 'wide'
                    )

st.title('🐱‍🐉SUMMARY')
st.sidebar.success("Select a page above.")

now = datetime.datetime.now()

@st.cache
def get_upload_data():
    upload = pd.read_excel(
        'upload_hist.xlsx'
    )
    return upload

upload_hist = get_upload_data()

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index = False).encode('utf-8')

csv = convert_df(upload_hist)

st.download_button(
        label="Download Upload History as CSV",
        data=csv,
        file_name='upload_hist_'+f'{now:%m%d%Y}' + '.csv',
        mime='text/csv',
)