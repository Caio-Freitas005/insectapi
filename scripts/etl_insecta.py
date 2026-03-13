import argparse
import os
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from huggingface_hub import HfApi

# Constants
CTFB_ZIP_URL = (
    "https://ipt.jbrj.gov.br/jbrj/archive.do?r=catalogo_taxonomico_da_fauna_do_brasil"
)
HF_REPO = "CaioFreitas05/br_insecta"

RANK_TRANSLATION = {
    "CLASSE": "CLASS",
    "SUB_CLASSE": "SUBCLASS",
    "SUPER_ORDEM": "SUPERORDER",
    "ORDEM": "ORDER",
    "SUB_ORDEM": "SUBORDER",
    "INFRA_ORDEM": "INFRAORDER",
    "SUPER_FAMILIA": "SUPERFAMILY",
    "FAMILIA": "FAMILY",
    "SUB_FAMILIA": "SUBFAMILY",
    "TRIBO": "TRIBE",
    "SUB_TRIBO": "SUBTRIBE",
    "GENERO": "GENUS",
    "SUB_GENERO": "SUBGENUS",
    "ESPECIE": "SPECIES",
    "SUB_ESPECIE": "SUBSPECIES",
}

LANGUAGE_TRANSLATION = {
    "PORTUGUES": "pt",
    "INGLES": "en",
    "ESPANHOL": "es",
    "FRANCES": "fr",
}


def download_and_extract_ctfb_files(data_dir: Path, debug_mode: bool) -> None:
    """Download the original ZIP folder and extract only the needed files."""
    taxon_path = data_dir / "taxon.txt"
    vernacular_path = data_dir / "vernacularname.txt"

    if debug_mode and taxon_path.exists() and vernacular_path.exists():
        print("[DEBUG MODE] Files .txt found locally. Skipping download.")
        return

    print(f"Downloading files at: {CTFB_ZIP_URL}...")
    zip_path = data_dir / "ctfb_data.zip"

    # Step 1: ZIP Folder download
    try:
        req = urllib.request.Request(
            CTFB_ZIP_URL, headers={"User-Agent": "Mozilla/5.0"}
        )
        with urllib.request.urlopen(req) as response, open(zip_path, "wb") as out_file:
            out_file.write(response.read())
        print("ZIP download finished!")
    except Exception as e:
        print(f"Error while downloading ZIP: {e}")
        return

    # Step 2: Extraction
    print("Extracting files...")
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for file in ["taxon.txt", "vernacularname.txt"]:
                if file in zip_ref.namelist():
                    zip_ref.extract(file, data_dir)
                    print(f" - {file} extracted sucessfully.")
                else:
                    print(f"Warning: {file} not found.")
    except zipfile.BadZipFile:
        print("Error: the folder is not a valid ZIP.")
    finally:
        # Step 3: Delete the ZIP folder
        if zip_path.exists():
            zip_path.unlink()
            print("Temporary folder removed.")


def process_taxons(data_dir: Path) -> pd.DataFrame:
    """Extracts, cleans, and translates taxonomic data for the Insecta class."""
    taxon_file = data_dir / "taxon.txt"
    if not taxon_file.exists():
        print(f"Error: The file '{taxon_file}' was not found.")
        return pd.DataFrame()

    taxon_columns = ["id", "scientificName", "taxonRank", "class", "taxonomicStatus"]
    taxon_dtypes: dict[str, str | Any] = {col: str for col in taxon_columns}

    print(f"Reading '{taxon_file.name}'...")
    try:
        df_taxon = pd.read_csv(
            taxon_file,
            sep="\t",
            usecols=taxon_columns,
            dtype=taxon_dtypes,  # type: ignore
        )
    except Exception as e:
        print(f"Failed to read '{taxon_file}': {e}")
        return pd.DataFrame()

    print("Filtering Insecta class and accepted names...")
    mask_insecta = df_taxon["class"] == "Insecta"
    mask_status = df_taxon["taxonomicStatus"] == "NOME_ACEITO"
    df_insecta = df_taxon[mask_insecta & mask_status].copy().reset_index(drop=True)
    df_insecta = df_insecta.drop(columns=["taxonomicStatus", "class"])

    print("Translating taxonRank values to English...")
    df_insecta["taxonRank"] = df_insecta["taxonRank"].replace(RANK_TRANSLATION)
    df_insecta = df_insecta.fillna("").astype(str)

    print(f"Total insects found: {len(df_insecta)}")
    return df_insecta


def process_vernacular_names(data_dir: Path, insects_ids: list[str]) -> pd.DataFrame:
    """Extracts and cleans vernacular names matching the provided insect IDs."""
    if not insects_ids:
        return pd.DataFrame()

    vernacular_file = data_dir / "vernacularname.txt"
    if not vernacular_file.exists():
        print(f"Warning: '{vernacular_file.name}' not found. Skipping.")
        return pd.DataFrame()

    vernacular_columns = ["id", "vernacularName", "language"]
    vernacular_dtypes: dict[str, str | Any] = {col: str for col in vernacular_columns}

    print(f"Reading '{vernacular_file.name}'...")
    try:
        df_vernacular = pd.read_csv(
            vernacular_file,
            sep="\t",
            usecols=vernacular_columns,
            dtype=vernacular_dtypes,  # type: ignore
        )
    except Exception as e:
        print(f"Failed to read '{vernacular_file}': {e}")
        return pd.DataFrame()

    print("Extracting and cleaning vernacular names for insects...")
    df_vernacular_insecta = (
        df_vernacular[df_vernacular["id"].isin(insects_ids)]
        .copy()
        .reset_index(drop=True)
    )
    df_vernacular_insecta = df_vernacular_insecta.astype(str)

    # Clean dirty data
    df_vernacular_insecta["vernacularName"] = df_vernacular_insecta[
        "vernacularName"
    ].str.replace(r"^\d+\.\s*", "", regex=True)

    print("Traslating and grouping names...")
    df_vernacular_insecta["language"] = df_vernacular_insecta["language"].replace(
        LANGUAGE_TRANSLATION
    )
    df_vernacular_insecta["name_with_lang"] = (
        df_vernacular_insecta["vernacularName"]
        + " ("
        + df_vernacular_insecta["language"]
        + ")"
    )

    # Group by id
    df_grouped = (
        df_vernacular_insecta.groupby("id")["name_with_lang"]
        .apply(list)
        .reset_index(name="vernacularNames")
    )
    print(f"Total vernacular names found: {len(df_vernacular_insecta)}")
    return df_grouped


def merge_datasets(
    df_taxons: pd.DataFrame, df_vernacular: pd.DataFrame
) -> pd.DataFrame:
    """Merge dataframes and clean null values."""
    print("Merging Taxons and Vernacular Names...")
    if not df_vernacular.empty:
        df_insecta = pd.merge(df_taxons, df_vernacular, on="id", how="left")
    else:
        df_insecta = df_taxons.copy()
        df_insecta["vernacularNames"] = None

    # Final cleanup to ensure no 'nan' strings ruin the JSON later
    df_insecta = df_insecta.replace("nan", np.nan)
    return df_insecta


def upload_to_huggingface(file_path: Path) -> None:
    """Upload to Hugging Face case the token exists."""
    hf_token = os.getenv("HF_TOKEN")

    if not hf_token:
        print("'HF_TOKEN' not found. Skipping upload to Hugging Face.")
        return

    print(f"Uploading '{file_path.name}' to Hugging Face ({HF_REPO})...")
    try:
        api = HfApi(token=hf_token)
        api.upload_file(
            path_or_fileobj=str(file_path),
            path_in_repo=file_path.name,
            repo_id=HF_REPO,
            repo_type="dataset",
        )
        print("Upload successfully finished!")
    except Exception as e:
        print(f"Error while uploading to Hugging Face: {e}")


def run_etl(debug_mode: bool = False) -> None:
    """Main orchestrator function for the ETL process."""
    print(f"--- Starting Data Processing (InsectAPI) | DEBUG: {debug_mode} ---\n")

    data_dir = Path("datasets")
    data_dir.mkdir(parents=True, exist_ok=True)

    output_parquet = data_dir / "insects.parquet"
    output_csv = data_dir / "insects.csv"

    # Step 1: Download and extract raw data files
    download_and_extract_ctfb_files(data_dir, debug_mode)

    # Step 2: Process Data
    df_taxons = process_taxons(data_dir)
    if df_taxons.empty:
        print("Process aborted: No taxon data available.")
        return

    insects_ids = df_taxons["id"].tolist()
    df_vernacular = process_vernacular_names(data_dir, insects_ids)

    # Step 3: Merge
    df_insecta = merge_datasets(df_taxons, df_vernacular)

    # Step 4: Save Local
    print(f"Saving Dataset with {len(df_insecta)} rows...")
    df_insecta.to_parquet(output_parquet, index=False)
    df_insecta.to_csv(output_csv, index=False)
    print(
        f"Files '{output_parquet.name}' and '{output_csv.name}' successfully saved!\n"
    )

    # Step 5: Upload (aborted if in debug mode)
    if debug_mode:
        print("[DEBUG MODE] Upload aborted.")
    else:
        upload_to_huggingface(output_parquet)

    print("\n--- Processing ended successfully! ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Pipeline for Insecta Data.")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Run in debug mode (skips download if files exist and skips upload)",
    )
    args = parser.parse_args()

    run_etl(debug_mode=args.debug)
