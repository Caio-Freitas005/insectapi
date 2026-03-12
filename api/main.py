import json
import shutil
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pandas as pd
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from huggingface_hub import hf_hub_download  # type: ignore

BASE_DIR = Path(__file__).resolve().parent.parent
DATASET_DIR = BASE_DIR / "datasets"
DATASET_PATH = DATASET_DIR / "insects.parquet"

HF_REPO= "CaioFreitas05/br_insecta"

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Starting API and loading data to memory...")

    DATASET_DIR.mkdir(parents=True, exist_ok=True)

    if not DATASET_PATH.exists():
        print("File not found locally. Downloading from cloud via Hugging Face SDK...")
        try:
            cached_file = hf_hub_download(
                repo_id=HF_REPO,
                filename="insects.parquet",
                repo_type="dataset",
            )
            shutil.copy(cached_file, DATASET_PATH)
            print("Download sucessfully finished!")
        except Exception as e:
            print(f"Critical error while downloading file: {e}")
    else:
        print("File found locally. Skipping download.")

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
    allow_origins=["http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def format_dataframe(df_subset: pd.DataFrame) -> list[dict[str, Any]]:
    """Convert DataFrame to a dictonary list"""
    if df_subset.empty:
        return []
    return json.loads(df_subset.to_json(orient="records", force_ascii=False))


@app.get("/")
def root():
    """Health Check route"""
    return {"message": "Welcome to InsectAPI!", "status": "online"}


@app.get("/insects/")
async def get_insects(
    limit: int = Query(10, description="results to return", ge=1, le=100),
) -> dict[str, Any]:
    """Return simple paginated insects list"""
    df: pd.DataFrame = app.state.df_insecta

    if df.empty:
        raise HTTPException(status_code=500, detail="Database unavailable.")

    df_subset = df.head(limit)
    results = format_dataframe(df_subset)

    return {"total": len(results), "data": results}


@app.get("/insects/search")
async def search_insects(
    q: str = Query(..., min_length=3, description="scientific name or vernacular name"),
) -> dict[str, Any]:
    """Return insects based on query"""
    df: pd.DataFrame = app.state.df_insecta

    if df.empty:
        raise HTTPException(status_code=500, detail="Database unavailable.")

    mask_scientific = df["scientificName"].str.contains(
        q, case=False, na=False, regex=False
    )

    mask_vernacular = (
        df["vernacularNames"]
        .astype(str)
        .str.contains(q, case=False, na=False, regex=False)
    )
    mask_vernacular = mask_vernacular & df["vernacularNames"].notna()

    df_results = df[mask_scientific | mask_vernacular].head(20)
    results = format_dataframe(df_results)

    return {"query:": q, "total": len(results), "results": results}
