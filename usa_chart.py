import pandas as pd
import mysql.connector
import datetime
import gspread as gs
import streamlit as st
import plotly.express as pe

@st.cache(allow_output_mutation=True)
def getdata():
    db = mysql.connector.Connect(
        host = 'sc-db.yes4all.internal',
        user = 'y4a_fa',
        passwd = 'das!dsoiVvmd09as')

    db_2 = mysql.connector.Connect(
        host = 'str-db.yes4all.internal',
        user = 'y4a_fin_duyduong',
        passwd = 'FIN!873bd712b')

    vendor_code = pd.read_sql_query(
        """
        SELECT code FROM sc_main.dim_vendor_yes4all
        """, con = db
    )
    vendor_code = vendor_code.loc[vendor_code.code.notna()].reset_index(drop= True)
    vendor_code= [i for i in vendor_code['code']]

    vendor_code_1 = [str(i)+"$" for i in vendor_code]
    vendor_code_2 = [str(i)+"#1-9@$" for i in vendor_code]
    vendor_code_3 = vendor_code_1 + vendor_code_2
    vendor_code_4 = str(vendor_code_3)
    vendor_code_4 = vendor_code_4.replace(",", "|").replace("'","").replace("[","").replace("]","").replace("#","[").replace("@","]").replace(' ','')

    overall = pd.read_sql_query(
        f"""
            SELECT
                a.invoice_number,
                a.invoice_date,
                a.invoice_creation_date,
                a.due_date,
                a.invoice_status,
                a.invoice_amount,
                b.freight_term,
                greatest(DATEDIFF(due_date, current_date()), 0) AS aging
            FROM
                bi_main.A02_RINV_OVERALL_MASTER a
            LEFT JOIN
                bi_main.A02_RINV_DETAIL_MASTER b
            ON a.invoice_number = b.invoice_number
            GROUP BY a.invoice_number, b.freight_term
            HAVING 
                a.invoice_number REGEXP '{vendor_code_4}'
            AND
                aging <> 0
        """, con = db
    )
    overall['invoice_amount'] = overall['invoice_amount'].astype(float)

    sc_di = pd.read_sql_query(
        """
        SELECT 
            po_name,
            CASE
                WHEN company = 'HMD' 
                    THEN COALESCE(invoice_amazon, CONCAT(po_name,'_', SUBSTR(invoice, 6, 3)), CONCAT(po_name, '_', company))
                ELSE 
                    COALESCE(invoice_amazon, CONCAT(po_name,'_', SUBSTR(invoice, 6, 3)))
                END invoice_amazon,
            invoice,
            company,
            country,
            invoice_date,
            amazon_payment_due_date AS due_date,
            greatest(DATEDIFF(amazon_payment_due_date, current_date()), 0) AS aging,

            ROUND(CAST(SUM(qty_vendor_load * invoice_cost) AS DOUBLE),2) AS invoice_amount

        FROM sc_main.sales_order_avc_di
        GROUP BY 1,2,3,4,5,6,7,8
        HAVING
            aging <> 0
        AND 
            COUNTRY = 'USA';
        """, con = db
    )

    sc_di["invoice_date"] = pd.to_datetime(sc_di["invoice_date"])
    sc_di["due_date"] = pd.to_datetime(sc_di["due_date"])

    cre = {
    "type": "service_account",
    "project_id": "red-grid-361311",
    "private_key_id": "cc5363f23831a5c6fc731d7802fb234ca7d71528",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC43eNR7Fu6AcuD\n4kAj1T5/ODWUSC2ApzNEZFeiPEJSfmMfxAVTrniSgkl3ej4YLdLdmae3DjUMa6LB\n2Oj0PR0tg2uFj79khu/booFg4fIe7qMIXCkWetF5oR94l+vt2l9Di2AVCylpvBaZ\n9aHorbO6lJ+4v2Bq4KNMFbPbIf853n3nRRggXNO9PI3JP6kQ+fu+1OfxGsfnHKks\nnOUdMkzSXiAwLZ2Pvbwy7AhgpXej3IBBs7m2cjZlTe5+nkIWPrrvn7LFMGdBnNGM\nc3Lg+CV2A4QHPuUDO7aq696OBHUZnGYOGgbxrFiuylVwNayXrONP9XUS2aq/niM6\nqdwdsguNAgMBAAECggEAFFvvaHrIwUvoT15v5OJbryQQFoP6auQ/C6WnVwke3rnn\njbyHP6eDxNLPZ3aRdxU410nQRCMu6W7DLmkGgrtmRrhl84AYumf3627So8pOYWSz\nlLydk8mz3xrE/gFxLLEzc5taWgbwKSJbNNpt2dOjawVqL6z3391cboknVRdB5evw\naQpIA32GuG6sRGbvU3fhHsRn4fr5OZEA523RDSvmz+Lp1iI7Exm/m2feeMSlnoIP\nUS45oX0Nm+7KOUJMjZ2f5Oqd05/xu1PBKdccls6pVzyhGRT7bXmEHIGCWadh+Hve\nTNck3aTQhm/uC3fYJY7r9oe6znPDEMGyLjvCqZ470QKBgQDi9Pc0m7j5ITl9VQ7l\n7cFABdSnMcYz4rHzKvFWRt+tdLCsCFNZ2YGVrqeTAqV4Ao3SbWSBqIdEIYumvmWC\n60KCM5XLlt1yD3omoBOT0VRysm/ItRWz0J0s/rEZEPwsAtPyFT02OIUA2QLa3tsz\nEJPQzcQdIhX7qisLMHY83dZtFQKBgQDQhhAQUvIywJEhMVdhWh7xpgqs8153hCbO\nUro+MrROQUtIz3p0VB2D63LWx2SX9yeQAkIsRV4YF96CIbVW6Ka47+By0LFvfAlK\nKNuSu+Mu5H5ikbOOyP5a7GOsNfGLlL2JPw7xpXY4j1a0Yu2P4HpvVWNccErAJNWB\nfiKizPnymQKBgQCEhf4DXhq4fkoPgSdd42LAe6ccqdfOXEUEdMLPWjsqEcVH7uSc\nxoEAYie/lAAC/5rIkM/rVfkGM4BUMUEdHTqMxIpngwRzoc311+sWmVjyUHctf2/Y\nu6vLpjzLIjvdhxkzdmtzybUANbeRDih4vOlTN5OYX1ruxQK1Werwx6h5sQKBgHxr\ngao708f0nqPvjPPJki0dpcEMdZFaiM0TD42NM4h97S3EnoxKpFHSavM8hgkmr7R9\npH5F5Z76nwmGk20H5HL0rjfTzgt3NxPIzTsAYM3aCI9H4JuR0jLuWYqnpThjx7Pc\nrE4DLEcTP+jiHl1605Man+7IEdIUKFDbsk7xfzIBAoGAGNkbsZ8g6ud525lDO52Q\nNcyFkElCHznr2V1iFjHx61/AJEuNZnywcjQ/4pA2aJ4F02F0MHlIGuQf27TdrzAX\nIMSgV9vVpbH2iNKscmVrne3bo0JTYtSuLRdKTPEBm2JDwA22hZHQ15Q6762HNppe\nMawvrNhNUBsjOeErciADPo8=\n-----END PRIVATE KEY-----\n",
    "client_email": "duy-625@red-grid-361311.iam.gserviceaccount.com",
    "client_id": "102329644455104275264",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/duy-625%40red-grid-361311.iam.gserviceaccount.com"
    }
    gc = gs.service_account_from_dict(cre)
    sh = gc.open('[Mirror reflected] Invoice List Submitted')

    #USA
    ws = sh.worksheet('Yes4All-USA')
    USA = pd.DataFrame(ws.get_all_values())
    USA.columns = USA.iloc[1,:]
    USA = USA.drop(axis = 0, index = [0,1])
    USA['Due date'] = pd.to_datetime(USA['Due date'])#, format = '%Y%M%d')
    USA['AGING'] = (USA['Due date'] - pd.Timestamp.today()).dt.days
    USA = USA.loc[USA['AGING'] > 0]
    USA['Inv No #'] = USA['Inv No #'].str.strip()
    USA['Inv Date'] = pd.to_datetime(USA['Inv Date']) #format = '%Y%M%d')
    USA['Invoice creation date'] = pd.to_datetime(USA['Invoice creation date'])#, format = '%Y%M%d')
    #USA['Invoice AMZ'] = USA['Invoice AMZ'].astype(float)
    #HMD
    ws_hmd = sh.worksheet('HMD')
    hmd = pd.DataFrame(ws_hmd.get_all_values())
    hmd.columns = hmd.iloc[0,:]
    hmd = hmd.drop(axis = 0, index = [0])
    hmd['Due date'] = pd.to_datetime(hmd['Due date'])#, format = '%Y%M%d')
    hmd['AGING'] = (hmd['Due date'] - pd.Timestamp.today()).dt.days
    hmd = hmd.loc[hmd['AGING'] > 0]
    hmd['Inv No #'] = hmd['Inv No #'].str.strip()

    usa_invoice = []
    hmd_invoice = []
    sale_di_invoice =[]
    overall_invoice = []

    for invoice in sc_di['invoice_amazon']:
        sale_di_invoice.append(invoice)
    for invoice in overall['invoice_number']:
        overall_invoice.append(invoice)
    for invoice in USA['Inv No #']:
        usa_invoice.append(invoice)
    for invoice in hmd['Inv No #']:
        hmd_invoice.append(invoice)

    avail_invoice = []
    for invoice in sale_di_invoice:
        avail_invoice.append(invoice)

    for invoice2 in overall_invoice:
        if invoice2 not in avail_invoice:
            avail_invoice.append(invoice2)
        else: pass

    for invoice3 in usa_invoice:
        if invoice3 not in avail_invoice and invoice3 not in overall_invoice:
            avail_invoice.append(invoice3)
        else: pass

    for invoice4 in hmd_invoice:
        if invoice4 not in avail_invoice and invoice4 not in overall_invoice and invoice4 not in usa_invoice:
            new_invoice.append(invoice4)
        else: pass

    upload_history = pd.read_sql_query(
        """
            SELECT * FROM y4a_fin.view_lsq_report_inv_uploaded;
        """, con = db_2
    )

    upload_list = upload_history['FILE_NAMES'].values.tolist()
    upload_list_str = str(upload_list)
    upload_list_str = upload_list_str.replace('.pdf', '').replace(' ','').replace("'", '')
    upload_list_str = [str(i) for i in upload_list_str.split(',') if (i.startswith('Inv') or i.startswith('inv'))  and 'Register' not in str(i)]

    # If there is a need to see how much have been submitted to LSQ -> Create a dataframe for those not starting with 'Invoice'
    # 'Invoice_' 'Invoice-' 'Invoice' 'Invocie_' 'Invocie-' 'Invocie' 'invoice_' 'invoice-' 'invoice'
    upload_list_str = str(upload_list_str)
    upload_list_str = upload_list_str.replace("Invoice_", '').replace("Invoice-",'').replace('Invoice','').replace("Invocie_", '').replace('Invocie-','').replace('Invocie','').replace('invoice_','').replace('invoice-','').replace('invoice','').replace('invocie_','').replace('invocie-','').replace('invocie','')
    upload_list_str = upload_list_str.replace('[', '').replace(']', '').replace("'", '')
    #Concat Missing List with upload_list

    # Create Dataframe
    upload_df = pd.DataFrame(upload_list_str.split(','), columns= ['Invoice_Number'])
    upload_df['Invoice_Number'] = upload_df['Invoice_Number'].str.strip()

    fcr_file = (r'C:\Users\duyduong\Desktop\Minh Duy\1. Work\2. Task\2. LSQ\2-WK\3-WK NOT YET UPLOADED\Po_extracted_file\14-po_extracted_11252022.xlsx')
    fcr_collection = pd.read_excel(f'{fcr_file}')
    fcr_collection = fcr_collection[['invoice_amazon', 'country' ,'fcr_number']]

    overall_2 = overall.copy()
    overall_2 = overall_2[['invoice_number','invoice_amount', 'invoice_status','invoice_date', 'invoice_creation_date', 'due_date','aging']]
    overall_2 = overall_2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    USA_2 = USA.copy()
    USA_2 = USA_2.reset_index(drop = True)
    USA_2['invoice_status'] = 'Submitted'
    USA_2 = USA_2[['Inv No #', 'Invoice AMZ', 'invoice_status', 'Inv Date', 'Invoice creation date', 'Due date', 'AGING']].rename(
    columns = ({'Inv No #': 'invoice_number', 'Invoice AMZ' : 'invoice_amount', 'Inv Date': 'invoice_date', 'Invoice creation date' :'invoice_creation_date', 'Due date': 'due_date', 'AGING': 'aging'})
    )
    USA_2 = USA_2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    sc_di_2 = sc_di.copy()
    sc_di_2['invoice_status'] = "NULL"
    sc_di_2['invoice_creation_date'] = "NULL"
    sc_di_2 = sc_di_2[['invoice_amazon','invoice_amount','invoice_status', 'invoice_date', 'invoice_creation_date', 'due_date','aging']].rename(
        columns = {'invoice_amazon' : 'invoice_number'}
    )
    sc_di_2 = sc_di_2.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    overall_3 = [i for i in overall_2['invoice_number']]
    USA_3 = [i for i in USA_2['invoice_number']]
    sc_di_3 = [i for i in sc_di_2['invoice_number']]

    # Check missing
    missing_1 = [i for i in USA_3 if i not in overall_3]
    missing_2 = [i for i in sc_di_3 if i not in overall_3 and i not in USA_3]
    append_1 = USA_2.loc[USA_2['invoice_number'].isin(missing_1)]
    append_2 = sc_di_2.loc[sc_di_2['invoice_number'].isin(missing_2)]
    final_lookup = pd.concat([overall_2, append_1, append_2])

    lookup_sale_di = pd.read_sql_query(
        """
            SELECT
                CASE
                    WHEN invoice_amazon IS NULL
                        AND company = 'HMD'
                        THEN COALESCE(invoice_amazon, CONCAT(po_name,'_', SUBSTR(invoice, 6, 3)), CONCAT(po_name, '_', company))
                    WHEN invoice_amazon IS NOT NULL
                        THEN invoice_amazon
                    ELSE COALESCE(invoice_amazon, CONCAT(po_name,'_', SUBSTR(invoice, 6, 3))) END AS invoice_amazon,
                invoice,
                company,
                country,
                CASE
                    WHEN is_shipped = 1
                        THEN 'SHIPPED'
                        ELSE 'NOT_SHIPPED'
                END ship_status,
                fcr
            FROM sc_main.sales_order_avc_di
        """, con = db
    )

    lookup_sale_di_2 = lookup_sale_di[['invoice_amazon','invoice' ,'ship_status', 'fcr']]
    master_invoice = final_lookup.copy()
    # Convert datatype
    master_invoice['invoice_amount'] = master_invoice['invoice_amount'].apply(lambda x: float(x.replace(',', '')) if type(x) == str else x)
    master_invoice['aging'] = master_invoice['aging'].apply(lambda x: int(x))
    # Merge with FCR Collection df
    master_invoice = master_invoice.merge(fcr_collection, how = 'left', left_on = ['invoice_number'], right_on = ['invoice_amazon'])
    master_invoice = master_invoice[
        ['invoice_number','invoice_amount','invoice_status', 'invoice_date', 'invoice_creation_date', 'due_date', 'aging', 'fcr_number']
        ].rename(
            columns = {'fcr_number' : 'fcr_collection'})
    # Add in Collection_Status
    master_invoice['fcr_collection_status'] = ['Collected' if x == x else 'Not yet' for x in master_invoice['fcr_collection']]
    # Merge with Lookup_sale_di_2
    master_invoice = master_invoice.merge(lookup_sale_di_2, how = 'left', left_on =['invoice_number'], right_on= ['invoice_amazon']).drop_duplicates(ignore_index = True)
    # Add in fcr_db_status
    master_invoice['fcr_db_status'] = ['NO' if x != x or str(x) == 'None'  else 'YES' for x in master_invoice['fcr']]
    # Re-order columns
    master_invoice = master_invoice[['invoice_number', 'invoice','invoice_amount', 'invoice_status',
        'invoice_date', 'invoice_creation_date', 'due_date', 
        'fcr_collection', 'fcr_collection_status',
        'fcr', 'fcr_db_status','aging']].rename(columns = {'fcr' : 'fcr_db', 'invoice' : 'invoice_vendor'})
    # Remove invoices that are uploaded
    master_invoice = master_invoice.loc[~master_invoice['invoice_number'].isin(upload_df['Invoice_Number'].to_list())].reset_index(drop= True)
    return master_invoice

master_invoice = getdata()

#AGING_ROUND
def custom_round(x, base=5):
    return int(base * round(float(x)/base))
master_invoice['aging_round'] = master_invoice['aging'].apply(lambda x: custom_round(x, base=5))

#---------------------STREAMLIT-----------------------------------------------------------------------------------------------------------------------

# st.set_page_config(page_title = 'USA',
#                     page_icon = ":money_with_wings:",
#                     layout = 'wide'
#                     )

#-------------SIDEBAR-------------------------------------------------------------
st.sidebar.header('Please Filter Here:')
# db_status = st.sidebar.multiselect(
#     "Select FCR Database Status",
#     options = master_invoice['fcr_db_status'].unique(),
#     default = master_invoice['fcr_db_status'].unique()
# )

collection_status = st.sidebar.multiselect(
    "Select FCR Collection Status:",
    options = master_invoice["fcr_collection_status"].unique(),
    default = master_invoice["fcr_collection_status"].unique()
)

aging_select = st.sidebar.slider(
    "Select Aging Group",
    #value = int(master_invoice['aging_round'].max()),
    int(master_invoice['aging_round'].min()), int(master_invoice['aging_round'].max()), value = (0,120), step = 5
)


# aging_select = st.sidebar.multiselect(
#     "Select Aging Group", 
#     options = master_invoice['aging_round'].unique(),
#     default = master_invoice['aging_round'].unique()
# )

df_selection = master_invoice.query(
    "aging_round >= @aging_select[0] & aging_round <= @aging_select[1]  & fcr_collection_status == @collection_status"
)

# df_selection = master_invoice.query(
#     "fcr_collection_status == @collection_status"
# )

# df_selection = master_invoice.query(
#     "fcr_db_status == @db_status & fcr_collection_status == @collection_status"
# )

#-----------MAIN PAGE----------------------
st.title(":money_with_wings: USA Available Invoice")
st.markdown("##")

# TOP KPI's
total_invoice = int(df_selection['invoice_number'].count())
total_amount = round(df_selection['invoice_amount'].sum(),2)
invoice_collected = int(df_selection.loc[df_selection['fcr_collection_status'] == 'Collected']['invoice_number'].count())
amount_collected = round(df_selection.loc[df_selection['fcr_collection_status'] == 'Collected']['invoice_amount'].sum(),2)

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
aging_by_collection = df_selection.groupby(
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
sun_df = df_selection.groupby(
    by = ['fcr_db_status','fcr_collection_status'], as_index = False
)[['invoice_number', 'invoice_amount']].aggregate({'invoice_number':'count', 'invoice_amount':'sum'})

fig_sun = pe.sunburst(
    sun_df, path = ['fcr_collection_status','fcr_db_status'],
    values = 'invoice_amount',
    template = 'plotly_dark',
    title = '<b>TOTAL INVOICE BY VALUE<b>',
    color = 'fcr_collection_status',
    color_discrete_map = {'Collected' : 'blue', 'Not yet' : 'red'}
)
fig_sun.update_traces(textinfo = 'label+percent entry')

st.plotly_chart(fig_sun)
st.plotly_chart(fig_collection)

#DATAFRAME -----------------------------------

st.dataframe(df_selection)

#DOWNLOAD BUTTON------------------------------

@st.cache
def convert_df(df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.to_csv(index = False).encode('utf-8')

csv = convert_df(master_invoice)
csv_selected = convert_df(df_selection)


left_column, right_column = st.columns(2)
with left_column:
    st.download_button(
        label="Download raw data as CSV",
        data=csv,
        file_name='master_invoice_usa.csv',
        mime='text/csv',
)
with right_column:
    st.download_button(
        label="Download filtered data as CSV",
        data=csv_selected,
        file_name='master_invoice_usa_filtered.csv',
        mime='text/csv',
)


# # HIDE ST STYLE----------------
# hide_st_style = """
#         <style>
#         #MainMenu {visibility: hidden,}
#         footer {visibility: hidden,}
#         header {visibility: hidden,}
#         </style>
# """

# st.markdown(hide_st_style, unsafe_allow_html= True)
