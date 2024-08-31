from app.database import Base
from sqlalchemy import Column, Float, String


class MergedData(Base):
    __tablename__ = "merged_data"

    order_id = Column(String, primary_key=True)
    transaction_type = Column(String)
    payment_type = Column(String, nullable=True)
    invoice_amount = Column(Float, nullable=True)
    net_amount = Column(Float, nullable=True)
    p_description = Column(String, nullable=True)
    order_date = Column(String)
    payment_date = Column(String, nullable=True)
