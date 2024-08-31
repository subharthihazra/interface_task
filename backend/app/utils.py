import logging

import pandas as pd
from fastapi import HTTPException

logging.basicConfig(filename="app/logs/elt_process.log", level=logging.INFO)


async def process_files(payment_report, mtr):
    try:
        payment_df = pd.read_csv(payment_report.file)
        print(payment_report.file)
        mtr_df = pd.read_excel(mtr.file)

        await process_mtr(mtr_df)
        await process_pr(payment_df)

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

        mtr_df.to_csv("app/uploaded_files/Transformed_MTR.csv", index=False)
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

        payment_df.to_csv(
            "app/uploaded_files/Transformed_Payment_Report.csv", index=False
        )
    except Exception as e:
        logging.error(f"Error processing payment report file: {e}")
