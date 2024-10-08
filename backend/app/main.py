from app.data_routes import router as data_router
from app.routes import router as api_router
from fastapi import FastAPI

app = FastAPI()

app.include_router(api_router)
app.include_router(data_router)


# Entry point for the application
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
