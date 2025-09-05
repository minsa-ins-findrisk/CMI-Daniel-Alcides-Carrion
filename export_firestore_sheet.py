import os, json
import firebase_admin
from firebase_admin import credentials, firestore, auth
import gspread
from gspread_dataframe import set_with_dataframe
import pandas as pd
from google.oauth2.service_account import Credentials
from datetime import datetime

# 🔹 Load Firebase credentials from GitHub Secret
firebase_key = json.loads(os.environ["FIREBASE_KEY"])
cred = credentials.Certificate(firebase_key)
firebase_admin.initialize_app(cred)
db = firestore.client()

# 🔹 Load Google Sheets credentials from GitHub Secret
sheets_key = json.loads(os.environ["GSHEETS_KEY"])
scopes = ["https://www.googleapis.com/auth/spreadsheets",
          "https://www.googleapis.com/auth/drive"]
gcreds = Credentials.from_service_account_info(sheets_key, scopes=scopes)
gc = gspread.authorize(gcreds)

# 🔹 Open Google Sheet
sh = gc.open_by_url("https://docs.google.com/spreadsheets/d/1T3y9UVs1GkbiZAv30UnohBGMKYpVFsxNuss_hCgs668/edit?gid=0#gid=0")
worksheet = sh.sheet1

# 🔹 Step 1: Export Firestore collection
docs = db.collection("CMI Daniel Alcides Carrion").stream()
data = []
for doc in docs:
    d = doc.to_dict()
    d["id"] = doc.id
    data.append(d)

df = pd.DataFrame(data)

# 🔹 Step 2: Fetch all users from Firebase Authentication
auth_users = []
page = auth.list_users()
while page:
    for user in page.users:
        reg_date = datetime.fromtimestamp(
            user.user_metadata.creation_timestamp / 1000.0
        ).strftime("%Y-%m-%d")
        auth_users.append({
            "id": user.uid,
            "fecha_de_registro": reg_date   # keep only registration date
        })
    page = page.get_next_page()

df_auth = pd.DataFrame(auth_users)

# 🔹 Step 3: Merge Firestore users with Auth users by uid (id)
df = df.merge(df_auth, on="id", how="left")

# 🔹 Step 4: Upload to Google Sheets
worksheet.clear()
set_with_dataframe(worksheet, df)
