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

now = datetime.datetime.now()

#READ DATA-----------------------------------------------------------------------------------
@st.cache
def get_inter_data():
    master_invoice_int = pd.read_excel(
        'master_inter.xlsx'
    )
    return master_invoice_int

master_invoice_int = get_inter_data()

#---------------------STREAMLIT-----------------------------------------------------------------------------------------------------------------------


#-------------SIDEBAR-------------------------------------------------------------
st.sidebar.header('Please Filter Here :wave::')
# db_status = st.sidebar.multiselect(
#     "Select FCR Database Status",
#     options = master_invoice['fcr_db_status'].unique(),
#     default = master_invoice['fcr_db_status'].unique()
# )

collection_status_int = st.sidebar.multiselect(
    "Select FCR Collection Status:",
    options = master_invoice_int["fcr_collection_status"].unique(),
    default = master_invoice_int["fcr_collection_status"].unique()
)

country_select_int = st.sidebar.multiselect(
    "Select International Countries:",
    options = master_invoice_int['country'].unique(),
    default = master_invoice_int['country'].unique()
)

aging_select_int = st.sidebar.slider(
    "Select Aging Group",
    #value = int(master_invoice['aging_round'].max()),
    int(master_invoice_int['aging_round'].min()), int(master_invoice_int['aging_round'].max()), value = (40,int(master_invoice_int['aging_round'].max())), step = 5
)


# aging_select = st.sidebar.multiselect(
#     "Select Aging Group", 
#     options = master_invoice['aging_round'].unique(),
#     default = master_invoice['aging_round'].unique()
# )

df_selection_int = master_invoice_int.query(
    "aging_round >= @aging_select_int[0] & aging_round <= @aging_select_int[1]  & fcr_collection_status == @collection_status_int & country == @country_select_int"
)

# df_selection = master_invoice.query(
#     "fcr_collection_status == @collection_status"
# )

# df_selection = master_invoice.query(
#     "fcr_db_status == @db_status & fcr_collection_status == @collection_status"
# )

#-----------MAIN PAGE----------------------
st.title(":earth_americas: International Available Invoice")
st.markdown("##")

# TOP KPI's
total_invoice = int(df_selection_int['invoice_number'].count())
total_amount = round(df_selection_int['invoice_amount'].sum(),2)
invoice_collected = int(df_selection_int.loc[df_selection_int['fcr_collection_status'] == 'Collected']['invoice_number'].count())
amount_collected = round(df_selection_int.loc[df_selection_int['fcr_collection_status'] == 'Collected']['invoice_amount'].sum(),2)

left_column, right_column = st.columns(2)
with left_column:
    st.subheader(f"Total Invoice Count: {total_invoice}")
with right_column:
    st.subheader(f"Collected Count: {invoice_collected}")


left_column, right_column = st.columns(2)
with left_column:
    st.subheader("Total Amount:")
    st.subheader(f"${total_amount:,}")
with right_column:
    st.subheader("Collected Amount:")
    st.subheader(f"${amount_collected:,}")

st.markdown("---")

# INVOICE BY AGING GROUP--------------------------------------------------------------------------
aging_by_collection = df_selection_int.groupby(
    by = ['aging_round','fcr_collection_status'], as_index= False
    )[['invoice_number','invoice_amount']].aggregate(
        {'invoice_amount' : 'sum', 'invoice_number' : 'count'}
        ).sort_values(by = 'aging_round').rename(columns = {'invoice_number': 'invoice_count'})

fig_collection = pe.bar(
                    aging_by_collection,
                    x= 'aging_round',
                    y= 'invoice_amount',
                    color = 'fcr_collection_status',
                    opacity = 0.7,
                    title = '<b>COLLECTION STATUS BY AGING GROUP<b>',
                    orientation = 'v',
                    barmode = 'relative',
                    text = 'invoice_amount',
                    template = 'plotly_dark',
                    color_discrete_map = {'Collected': 'blue', 'Not yet': 'red'},
                    hover_data = {
                                'aging_round' : False,
                                'fcr_collection_status': False,
                                'invoice_amount':':.2f',
                                'invoice_count': True
                                },
                    labels = {'invoice_amount':'AMOUNT', 'aging_round' : 'AGING', 'invoice_count': 'COUNT'}
)
fig_collection.update_traces(texttemplate  = '%{text:.2s}')
fig_collection.update_yaxes(showticklabels = False)
fig_collection.update_layout(
    uniformtext_minsize = 9.5, 
    uniformtext_mode ='show', 
    xaxis = dict(tickmode = 'linear', tick0 = 5, dtick = 5),
    yaxis = dict(showgrid = False),
    plot_bgcolor = "rgba(0,0,0,0)"
)
fig_collection.update_traces(textposition = 'outside')

# SUNBURST CHART--------------------
sun_df = df_selection_int.groupby(
    by = ['fcr_collection_status','aging_round','country'], as_index = False
)[['invoice_number', 'invoice_amount']].aggregate({'invoice_number':'count', 'invoice_amount':'sum'})

fig_sun = pe.sunburst(
    sun_df, path = ['fcr_collection_status','country', 'aging_round'],
    values = 'invoice_amount',
    template = 'plotly_dark',
    title = '<b>TOTAL INVOICE BY VALUE<b>',
    color = 'fcr_collection_status',
    color_discrete_map = {'Collected' : 'blue', 'Not yet' : 'red'}
)
fig_sun.update_traces(textinfo = 'label+percent entry')

# col1, col2  = st.columns(2)
# col1.plotly_chart(fig_sun)
# col2.plotly_chart(fig_collection)

#DATAFRAME -----------------------------------

# st.dataframe(df_selection)

#DOWNLOAD BUTTON------------------------------

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index = False).encode('utf-8')

csv = convert_df(master_invoice_int)
csv_selected = convert_df(df_selection_int)

col1, col2  = st.columns(2)
col1.dataframe(df_selection_int.groupby(['country'])['invoice_amount'].sum().sort_values(ascending = False))
col2.dataframe(df_selection_int.pivot_table(index = 'country', columns = 'aging_round', values = 'invoice_amount',aggfunc = 'sum'))

st.plotly_chart(fig_sun)


st.plotly_chart(fig_collection)
st.dataframe(df_selection_int)

col1, col2  = st.columns(2)
col1.download_button(
        label="Download raw data as CSV",
        data=csv,
        file_name='master_invoice_inter_'+f'{now:%m%d%Y}' + '.csv',
        mime='text/csv',
)
col2.download_button(
        label="Download filtered data as CSV",
        data=csv_selected,
        file_name='master_invoice_inter_filtered_'+f'{now:%m%d%Y}' + '.csv',
        mime='text/csv',
)

# left_column, right_column = st.columns(2)
# with left_column:
#     st.download_button(
#         label="Download raw data as CSV",
#         data=csv,
#         file_name='master_invoice_usa.csv',
#         mime='text/csv',
# )
# with right_column:
#     st.download_button(
#         label="Download filtered data as CSV",
#         data=csv_selected,
#         file_name='master_invoice_usa_filtered.csv',
#         mime='text/csv',
# )


# # HIDE ST STYLE----------------
# hide_st_style = """
#         <style>
#         #MainMenu {visibility: hidden,}
#         footer {visibility: hidden,}
#         header {visibility: hidden,}
#         </style>
# """

# st.markdown(hide_st_style, unsafe_allow_html= True)
