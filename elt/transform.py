import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()


db_url = os.getenv("DATABASE_URL")

engine = create_engine(db_url)

# Load the merged sheet into a DataFrame
df = pd.read_excel("./files/Merged_Report5.xlsx")

# Display the first few rows of the DataFrame
print(df.head())

# 1. Filter out rows with empty Order IDs
filtered_df = df[df["Order Id"].notna()]

df["Net Amount"] = df["Net Amount"].str.replace(",", "").astype(float)

# 2. Create summary for transactions with empty Order IDs
summary_df = (
    df[df["Order Id"].isna()]
    .groupby("P_Description")
    .agg({"Net Amount": "sum"})
    .reset_index()
)

# Save summary to the database
summary_df.to_sql("transaction_summary", engine, if_exists="replace", index=False)


# 3. Group by Order ID and classify transactions
def classify_transactions(row):
    if pd.isna(row["Order Id"]):
        return "No Order ID"
    elif row["Transaction Type"] == "Return" and pd.notna(row["Invoice Amount"]):
        return "Return"
    elif (
        row["Transaction Type"] == "Payment"
        and pd.notna(row["Net Amount"])
        and float(row["Net Amount"].replace(",", "")) < 0
    ):
        return "Negative Payout"
    elif (
        pd.notna(row["Order Id"])
        and pd.notna(row["Net Amount"])
        and pd.notna(row["Invoice Amount"])
    ):
        return "Order & Payment Received"
    elif (
        pd.notna(row["Order Id"])
        and pd.notna(row["Net Amount"])
        and pd.isna(row["Invoice Amount"])
    ):
        return "Order Not Applicable but Payment Received"
    elif (
        pd.notna(row["Order Id"])
        and pd.isna(row["Net Amount"])
        and pd.notna(row["Invoice Amount"])
    ):
        return "Payment Pending"
    else:
        return "Unclassified"


filtered_df["Classification"] = filtered_df.apply(classify_transactions, axis=1)

# Save the classified data to the database
filtered_df.to_sql("classified_data", engine, if_exists="replace", index=False)

# 4. Generate additional datasets based on the classification
removal_orders = filtered_df[filtered_df["Order Id"] == "No Order ID"]
return_data = filtered_df[filtered_df["Classification"] == "Return"]
negative_payouts = filtered_df[filtered_df["Classification"] == "Negative Payout"]
order_payment_received = filtered_df[
    filtered_df["Classification"] == "Order & Payment Received"
]
order_not_applicable_payment_received = filtered_df[
    filtered_df["Classification"] == "Order Not Applicable but Payment Received"
]
payment_pending = filtered_df[filtered_df["Classification"] == "Payment Pending"]

# Save additional datasets to the database
removal_orders.to_sql("removal_orders", engine, if_exists="replace", index=False)
return_data.to_sql("return_data", engine, if_exists="replace", index=False)
negative_payouts.to_sql("negative_payouts", engine, if_exists="replace", index=False)
order_payment_received.to_sql(
    "order_payment_received", engine, if_exists="replace", index=False
)
order_not_applicable_payment_received.to_sql(
    "order_not_applicable_payment_received", engine, if_exists="replace", index=False
)
payment_pending.to_sql("payment_pending", engine, if_exists="replace", index=False)


# 5. (Optional) Measure Tolerance
def check_tolerance(row):
    if pd.notna(row["Net Amount"]) and pd.notna(row["Invoice Amount"]):
        pna = row["Net Amount"]
        invoice_amount = row["Invoice Amount"]
        percentage = (pna / invoice_amount) * 100
        if 0 < pna < 300:
            return "Within Tolerance" if percentage > 50 else "Tolerance Breached"
        elif 301 < pna < 500:
            return "Within Tolerance" if percentage > 45 else "Tolerance Breached"
        elif 501 < pna < 900:
            return "Within Tolerance" if percentage > 43 else "Tolerance Breached"
        elif 901 < pna < 1500:
            return "Within Tolerance" if percentage > 38 else "Tolerance Breached"
        elif pna > 1500:
            return "Within Tolerance" if percentage > 30 else "Tolerance Breached"
    return "No Invoice Amount"


filtered_df["Tolerance"] = filtered_df.apply(check_tolerance, axis=1)

# Save tolerance data to the database
tolerance_data = filtered_df[
    [
        "Order Id",
        "Transaction Type",
        "Payment Type",
        "Invoice Amount",
        "Net Amount",
        "Tolerance",
    ]
]
tolerance_data.to_sql("tolerance_data", engine, if_exists="replace", index=False)
