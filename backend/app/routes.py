from app.utils import process_files
from fastapi import APIRouter, File, UploadFile

router = APIRouter()


@router.get("/")
def read_root():
    return {"Hello": "World"}


@router.post("/upload/")
async def upload_files(
    payment_report: UploadFile = File(...), mtr: UploadFile = File(...)
):
    return await process_files(payment_report, mtr)
