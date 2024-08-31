import logging

import pandas as pd
from app.database import engine
from fastapi import HTTPException

logging.basicConfig(filename="app/logs/elt_process.log", level=logging.INFO)


async def process_files(payment_report, mtr):
    try:
        mtr_df = pd.read_excel(mtr.file)
        payment_df = pd.read_csv(payment_report.file)

        mtr_df = await process_mtr(mtr_df)
        payment_df = await process_pr(payment_df)

        merged_df = await generate_merged(mtr_df, payment_df)

        await store_merged_db(merged_df)
        await transform_merged_data_and_store_db(merged_df)

        logging.info("Data uploaded and processed successfully")
        return {"message": "Files uploaded and processed successfully"}
    except Exception as e:
        logging.error(f"Error processing files: {e}")
        raise HTTPException(status_code=400, detail=f"Error processing files: {str(e)}")


async def process_mtr(mtr_df):
    try:
        mtr_df = mtr_df[mtr_df["Transaction Type"] != "Cancel"]

        mtr_df["Transaction Type"] = mtr_df["Transaction Type"].replace(
            "Refund", "Return"
        )

        mtr_df["Transaction Type"] = mtr_df["Transaction Type"].replace(
            "FreeReplacement", "Return"
        )

        return mtr_df

        # mtr_df.to_csv("app/uploaded_files/Transformed_MTR.csv", index=False)
    except Exception as e:
        logging.error(f"Error processing mtr file: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error processing mtr file: {str(e)}"
        )


async def process_pr(payment_df):
    try:
        payment_df = payment_df[payment_df["type"] != "Transfer"]

        payment_df.rename(columns={"type": "Payment Type"}, inplace=True)

        payment_df["Payment Type"] = payment_df["Payment Type"].replace(
            {
                "Ajdustment": "Order",
                "FBA Inventory Fee": "Order",
                "Fulfilment Fee Refund": "Order",
                "Service Fee": "Order",
                "Refund": "Return",
            }
        )

        payment_df["Transaction Type"] = "Payment"

        return payment_df

        # payment_df.to_csv(
        #     "app/uploaded_files/Transformed_Payment_Report.csv", index=False
        # )
    except Exception as e:
        logging.error(f"Error processing payment report file: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error processing payment report file: {str(e)}"
        )


async def generate_merged(mtr_df, payment_df):
    try:
        # print("fff",mtr_df,payment_df)
        columns = [
            "Order Id",
            "Transaction Type",
            "Payment Type",
            "Invoice Amount",
            "Net Amount",
            "P_Description",
            "Order Date",
            "Payment Date",
        ]
        df = pd.DataFrame(columns=columns)

        mtr_rows = [map_mtr_row(row) for _, row in mtr_df.iterrows()]
        payment_rows = [map_payment_row(row) for _, row in payment_df.iterrows()]

        mtr_df_final = pd.DataFrame(mtr_rows, columns=columns)
        payment_df_final = pd.DataFrame(payment_rows, columns=columns)

        df = pd.concat([mtr_df_final, payment_df_final], ignore_index=True)

        return df

    except Exception as e:
        logging.error(f"Error merging report files: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error merging report files: {str(e)}"
        )


def map_mtr_row(row):
    return {
        "Order Id": row.get("Order Id", ""),
        "Transaction Type": row.get("Transaction Type", ""),
        "Payment Type": "",
        "Invoice Amount": row.get("Invoice Amount", ""),
        "Net Amount": "",
        "P_Description": "",
        "Order Date": row.get("Order Date", ""),
        "Payment Date": "",
    }


def map_payment_row(row):
    return {
        "Order Id": row.get("order id", ""),
        "Transaction Type": row.get("Transaction Type", ""),
        "Payment Type": row.get("Payment Type", ""),
        "Invoice Amount": "",
        "Net Amount": row.get("total", ""),
        "P_Description": row.get("description", ""),
        "Order Date": "",
        "Payment Date": row.get("date/time", ""),
    }


async def store_merged_db(df):
    try:
        df.to_sql("merged_data", engine, if_exists="replace", index=False)
    except Exception as e:
        logging.error(f"Error storing data to db: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error storing data to db: {str(e)}"
        )


async def transform_merged_data_and_store_db(df):
    try:

        df["Net Amount"] = (
            df["Net Amount"].str.replace(",", "").astype(float)
            if len(df["Net Amount"].str.strip()) == 0
            else 0
        )
        df["Invoice Amount"] = (
            df["Invoice Amount"].str.replace(",", "").astype(float)
            if len(df["Invoice Amount"].str.strip()) == 0
            else 0
        )

        filtered_df = df[df["Order Id"].notna()]

        summary_df = (
            df[df["Order Id"].isna()]
            .groupby("P_Description")
            .agg({"Net Amount": "sum"})
            .reset_index()
        )

        summary_df.to_sql(
            "transaction_summary", engine, if_exists="replace", index=False
        )

        filtered_df["Classification"] = filtered_df.apply(classify_transactions, axis=1)
        # print("dggggggggggggggggggggggggggggss")

        filtered_df.to_sql("classified_data", engine, if_exists="replace", index=False)

        removal_orders = filtered_df[filtered_df["Order Id"] == "No Order ID"]
        return_data = filtered_df[filtered_df["Classification"] == "Return"]
        negative_payouts = filtered_df[
            filtered_df["Classification"] == "Negative Payout"
        ]
        order_payment_received = filtered_df[
            filtered_df["Classification"] == "Order & Payment Received"
        ]
        order_not_applicable_payment_received = filtered_df[
            filtered_df["Classification"] == "Order Not Applicable but Payment Received"
        ]
        payment_pending = filtered_df[
            filtered_df["Classification"] == "Payment Pending"
        ]

        removal_orders.to_sql(
            "removal_orders", engine, if_exists="replace", index=False
        )
        return_data.to_sql("return_data", engine, if_exists="replace", index=False)
        negative_payouts.to_sql(
            "negative_payouts", engine, if_exists="replace", index=False
        )
        order_payment_received.to_sql(
            "order_payment_received", engine, if_exists="replace", index=False
        )
        order_not_applicable_payment_received.to_sql(
            "order_not_applicable_payment_received",
            engine,
            if_exists="replace",
            index=False,
        )
        payment_pending.to_sql(
            "payment_pending", engine, if_exists="replace", index=False
        )

        filtered_df["Tolerance"] = filtered_df.apply(check_tolerance, axis=1)

        # print("dggggwwwwwwwwwwwwgss")

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
        tolerance_data.to_sql(
            "tolerance_data", engine, if_exists="replace", index=False
        )

        # print("xxx",tolerance_data)

    except Exception as e:
        logging.error(f"Error transforming data to db: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error transforming data to db: {str(e)}"
        )


def classify_transactions(row):
    if pd.isna(row["Order Id"]):
        return "No Order ID"
    elif row["Transaction Type"] == "Return" and pd.notna(row["Invoice Amount"]):
        return "Return"
    elif (
        row["Transaction Type"] == "Payment"
        and pd.notna(row["Net Amount"])
        and float(row["Net Amount"]) < 0
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


def check_tolerance(row):
    if pd.notna(row["Net Amount"]) and pd.notna(row["Invoice Amount"]):
        pna = row["Net Amount"]
        invoice_amount = row["Invoice Amount"]
        if invoice_amount == 0:
            return
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
