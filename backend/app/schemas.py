from typing import Optional

from pydantic import BaseModel


class MergedDataSchema(BaseModel):
    order_id: Optional[str]
    transaction_type: Optional[str]
    payment_type: Optional[str]
    invoice_amount: Optional[float]
    net_amount: Optional[float]
    p_description: Optional[str]
    order_date: Optional[str]
    payment_date: Optional[str]

    class Config:
        from_attributes = True  # Correct config option for Pydantic V2
