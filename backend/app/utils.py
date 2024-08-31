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

        print("dddddddddddddddd")
        print(merged_df)

        await store_merged_db(merged_df)

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
