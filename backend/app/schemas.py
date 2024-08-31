from pydantic import BaseModel, Field


class DataModel(BaseModel):
    order_id: str = Field(..., alias="Order_Id")
    transaction_type: str = Field(..., alias="Transaction_Type")
    payment_type: str = Field(None, alias="Payment_Type")
    invoice_amount: float = Field(None, alias="Invoice_Amount")
    net_amount: float = Field(None, alias="Net_Amount")
    p_description: str = Field(None, alias="P_Description")
    order_date: str = Field(..., alias="Order_Date")
    payment_date: str = Field(None, alias="Payment_Date")

    class Config:
        orm_mode = True
