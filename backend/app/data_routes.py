from app.database import engine
from app.models import MergedData
from app.schemas import MergedDataSchema
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

router = APIRouter()


def get_db():
    db = Session(bind=engine)
    try:
        yield db
    finally:
        db.close()


@router.get("/transaction-summary")
async def fetch_transaction_summary(db: Session = Depends(get_db)):
    try:
        result = db.execute("SELECT * FROM transaction_summary").fetchall()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching transaction summary: {str(e)}"
        )


@router.get("/classified-data")
async def fetch_classified_data(db: Session = Depends(get_db)):
    try:
        result = db.execute("SELECT * FROM classified_data").fetchall()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching classified data: {str(e)}"
        )


@router.get("/data/{classification}")
async def fetch_data_by_classification(
    classification: str, db: Session = Depends(get_db)
):
    try:
        query = f"SELECT * FROM {classification}"
        result = db.execute(query).fetchall()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching data by classification: {str(e)}"
        )


@router.get("/tolerance-data")
async def fetch_tolerance_data(db: Session = Depends(get_db)):
    try:
        result = db.execute("SELECT * FROM tolerance_data").fetchall()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Error fetching tolerance data: {str(e)}"
        )
