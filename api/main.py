from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_PATH = BASE_DIR / "datasets" / "insects.parquet"


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting API and loading data to memory...")
    try:
        df = pd.read_parquet(DATASET_PATH)
        app.state.df_insecta = df
        print("Data successfully loaded!")
    except Exception as e:
        print(f"An error has occurred while loading data: {e}")
        app.state.df_insecta = pd.DataFrame()

    yield

    print("Finishing API and cleaning memory...")
    app.state.df_insecta = None


app = FastAPI(
    title="InsectAPI",
    description="Read-Only API for CTFB Insecta taxonomic data.",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    """Health Check route"""
    return {"message": "Welcome to InsectAPI!", "status": "online"}


@app.get("/insects/")
async def get_insects(
    limit: int = Query(10, description="Results to return", ge=1, le=100),
) -> dict[str, Any]:
    """Return simple paginated insects list"""
    df: pd.DataFrame = app.state.df_insecta

    if df.empty:
        raise HTTPException(status_code=500, detail="Database unavailable.")

    df_subset = df.head(limit)
    df_clean = df_subset.where(pd.notnull(df_subset), None)  # type: ignore

    results = df_clean.to_dict(orient="records")

    return {"total": len(results), "data": results}
