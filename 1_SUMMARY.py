import pandas as pd
import streamlit as st
import plotly.express as pe
import xlrd
import openpyxl

st.set_page_config(page_title = 'Yes4All_Invoice',
                    page_icon = ":money_with_wings:",
                    layout = 'wide'
                    )

st.title('🐱‍🐉SUMMARY')
st.sidebar.success("Select a page above.")