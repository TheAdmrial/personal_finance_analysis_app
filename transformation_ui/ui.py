# app.py

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# ===== CONFIG =====
DATABASE_URI = "postgresql+psycopg2://username:password@localhost:5432/your_database"
CATEGORY_MEMORY_FILE = "category_memory.csv"
COMPANY_MEMORY_FILE = "company_memory.csv"

st.set_page_config(page_title="Personal Finance Loader", layout="wide")
st.title("💰 Personal Finance Data Loader")

# ===== MEMORY FILE HELPERS =====
def load_memory(file_path):
    if os.path.exists(file_path):
        return pd.read_csv(file_path)
    return pd.DataFrame(columns=["description", "value"])

def save_memory(file_path, df):
    df.to_csv(file_path, index=False)

# Load saved memories
category_memory = load_memory(CATEGORY_MEMORY_FILE)
company_memory = load_memory(COMPANY_MEMORY_FILE)

# ===== FILE UPLOAD =====
uploaded_file = st.file_uploader("Upload a CSV file", type=["csv"])

if uploaded_file:
    # Load file into DataFrame
    try:
        df = pd.read_csv(uploaded_file)
        st.success(f"Loaded '{uploaded_file.name}' successfully!")
    except Exception as e:
        st.error(f"Error reading file: {e}")
        st.stop()

    st.subheader("Raw Data Preview")
    st.dataframe(df.head())

    # ===== STEP 2: YOUR CLEANING LOGIC =====
    # Example cleaning steps — replace with your actual logic
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]
    df.dropna(how="all", inplace=True)

    # Example date parsing
    date_cols = [col for col in df.columns if "date" in col.lower()]
    for col in date_cols:
        try:
            df[col] = pd.to_datetime(df[col])
        except:
            pass

    # ===== STEP 3: FIND UNCATEGORIZED TRANSACTIONS =====
    if "category" not in df.columns:
        df["category"] = None
    if "company" not in df.columns:
        df["company"] = None
    if "description" not in df.columns:
        st.error("Your data must have a 'description' column for memory matching.")
        st.stop()

    uncategorized = df[df["category"].isna() | df["category"].eq("") |
                       df["company"].isna() | df["company"].eq("")].copy()

    if not uncategorized.empty:
        st.subheader("📝 Categorize Transactions")

        updated_rows = []
        for idx, row in uncategorized.iterrows():
            desc = row["description"]

            # === CATEGORY AUTOFILL ===
            cat_default = ""
            match_cat = category_memory[category_memory["description"] == desc]
            if not match_cat.empty:
                cat_default = match_cat["value"].iloc[0]

            category = st.selectbox(
                f"Category for: {desc}",
                options=[""] + sorted(category_memory["value"].unique().tolist()),
                index=([""] + sorted(category_memory["value"].unique().tolist())).index(cat_default) if cat_default in category_memory["value"].values else 0,
                key=f"cat_{idx}"
            )

            # Allow typing a new category
            if category == "":
                category = st.text_input(f"New Category for: {desc}", key=f"cat_new_{idx}")

            # === COMPANY AUTOFILL ===
            comp_default = ""
            match_comp = company_memory[company_memory["description"] == desc]
            if not match_comp.empty:
                comp_default = match_comp["value"].iloc[0]

            company = st.selectbox(
                f"Company for: {desc}",
                options=[""] + sorted(company_memory["value"].unique().tolist()),
                index=([""] + sorted(company_memory["value"].unique().tolist())).index(comp_default) if comp_default in company_memory["value"].values else 0,
                key=f"comp_{idx}"
            )

            # Allow typing a new company
            if company == "":
                company = st.text_input(f"New Company for: {desc}", key=f"comp_new_{idx}")

            updated_rows.append((idx, desc, category, company))

        if st.button("Save Categories"):
            for idx, desc, category, company in updated_rows:
                if category:
                    df.at[idx, "category"] = category
                    # Remember mapping
                    if not ((category_memory["description"] == desc) & (category_memory["value"] == category)).any():
                        category_memory = pd.concat([category_memory, pd.DataFrame({"description": [desc], "value": [category]})], ignore_index=True)

                if company:
                    df.at[idx, "company"] = company
                    # Remember mapping
                    if not ((company_memory["description"] == desc) & (company_memory["value"] == company)).any():
                        company_memory = pd.concat([company_memory, pd.DataFrame({"description": [desc], "value": [company]})], ignore_index=True)

            # Save updated memories
            save_memory(CATEGORY_MEMORY_FILE, category_memory)
            save_memory(COMPANY_MEMORY_FILE, company_memory)

            still_blank = df[df["category"].isna() | df["category"].eq("") |
                             df["company"].isna() | df["company"].eq("")]
            if still_blank.empty:
                st.success("✅ All transactions categorized!")
            else:
                st.warning("⚠ Some transactions are still uncategorized.")
    else:
        st.info("No uncategorized transactions found — ready to upload.")

    # ===== STEP 4: SAVE TO DATABASE =====
    if st.button("Upload to Database"):
        still_blank = df[df["category"].isna() | df["category"].eq("") |
                         df["company"].isna() | df["company"].eq("")]
        if not still_blank.empty:
            st.error("Cannot upload — some transactions are still uncategorized.")
        else:
            try:
                engine = create_engine(DATABASE_URI)
                df.to_sql("finance_data", engine, if_exists="append", index=False)
                st.success("Data uploaded to PostgreSQL successfully!")
            except Exception as e:
                st.error(f"Error saving to database: {e}")
