import os

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.routes import products

_FRONTEND = os.path.join(os.path.dirname(__file__), "frontend")

app = FastAPI(
    title="Kroger Price Intelligence",
    description="Search Kroger products and compare prices across Houston locations.",
    version="0.1.0",
)

app.include_router(products.router, prefix="/api/v1")

# Serve the frontend HTML
@app.get("/", include_in_schema=False)
async def root():
    return FileResponse(os.path.join(_FRONTEND, "index.html"))
