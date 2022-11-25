import pandas as pd
import mysql.connector
import gspread as gs
# import streamlit as st

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

ws_hmd = sh.worksheet('HMD')
hmd = pd.DataFrame(ws_hmd.get_all_values())
hmd.columns = hmd.iloc[0,:]
hmd = hmd.drop(axis = 0, index = [0])
hmd['Due date'] = pd.to_datetime(hmd['Due date'])#, format = '%Y%M%d')
hmd['AGING'] = (hmd['Due date'] - pd.Timestamp.today()).dt.days
hmd = hmd.loc[hmd['AGING'] > 0]
hmd['Inv No #'] = hmd['Inv No #'].str.strip()

print(hmd)