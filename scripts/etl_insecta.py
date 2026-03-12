from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

# Global Constants for translations
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


def process_taxons(data_dir: Path) -> pd.DataFrame:
    """Extracts, cleans, and translates taxonomic data for the Insecta class."""
    taxon_file = data_dir / "taxon.txt"

    if not taxon_file.exists():
        print(f"Error: The file '{taxon_file}' was not found.")
        return pd.DataFrame()

    taxon_columns = ["id", "scientificName", "taxonRank", "class", "taxonomicStatus"]
    taxon_dtypes: dict[str, str | Any] = {
        "id": str,
        "scientificName": str,
        "taxonRank": str,
        "class": str,
        "taxonomicStatus": str,
    }

    print(f"Reading '{taxon_file}'...")
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

    # Filter masks
    mask_insecta = df_taxon["class"] == "Insecta"
    mask_status = df_taxon["taxonomicStatus"] == "NOME_ACEITO"

    df_insecta = df_taxon[mask_insecta & mask_status].copy().reset_index(drop=True)

    # Remove columns
    df_insecta = df_insecta.drop(columns=["taxonomicStatus", "class"])

    print("Translating taxonRank values to English...")
    df_insecta["taxonRank"] = df_insecta["taxonRank"].replace(RANK_TRANSLATION)

    # Cleaning and type casting
    df_insecta = df_insecta.fillna("").astype(str)

    print(f"Total insects found: {len(df_insecta)}")

    return df_insecta


def process_vernacular_names(data_dir: Path, insects_ids: list[str]) -> pd.DataFrame:
    """Extracts and cleans vernacular names matching the provided insect IDs."""
    if not insects_ids:
        print("No insect IDs provided. Skipping vernacular names processing.")
        return pd.DataFrame()

    vernacular_file = data_dir / "vernacularname.txt"

    if not vernacular_file.exists():
        print(f"Error: The file '{vernacular_file}' was not found. Skipping.")
        return pd.DataFrame()

    vernacular_columns = ["id", "vernacularName", "language"]
    vernacular_dtypes: dict[str, str | Any] = {
        "id": str,
        "vernacularName": str,
        "language": str,
    }

    print(f"Reading '{vernacular_file}'...")
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

    # Cleaning and type casting
    df_vernacular_insecta = df_vernacular_insecta.astype(str)

    # Clean dirty data
    df_vernacular_insecta["vernacularName"] = df_vernacular_insecta[
        "vernacularName"
    ].str.replace(r"^\d+\.\s*", "", regex=True)

    print("Traslating and grouping names...")

    # Translate language to ISO acronym
    df_vernacular_insecta["language"] = df_vernacular_insecta["language"].replace(
        LANGUAGE_TRANSLATION
    )

    # Create column with normalized name and language
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


def extract_ctfb_data() -> None:
    """Main orchestrator function for the ETL process."""
    print("--- Starting Data Processing (InsectAPI) ---\n")

    data_dir = Path("datasets")
    output_parquet = data_dir / "insects.parquet"
    output_csv = data_dir / "insects.csv"

    # Step 1: Process taxons
    df_taxons = process_taxons(data_dir)

    if df_taxons.empty:
        print("Process aborted: No taxon data available.")
        return

    insects_ids = df_taxons["id"].tolist()

    # Step 2: Process Vernacular Names using the IDs
    df_vernacular = process_vernacular_names(data_dir, insects_ids)

    # Step 3: (Left Join)
    print("\nMerging Taxons and Vernacular Names...")
    if not df_vernacular.empty:
        df_insecta = pd.merge(df_taxons, df_vernacular, on="id", how="left")
    else:
        # Fallback case there isn't vernacular names
        df_insecta = df_taxons.copy()
        df_insecta["vernacularName"] = None
        df_insecta["language"] = None

    # Step 4: Final cleanup to ensure no 'nan' strings ruin the JSON later
    df_insecta = df_insecta.replace("nan", np.nan)

    # Step 5: Save the definitive file
    print(f"Saving Dataset with {len(df_insecta)} rows...")
    df_insecta.to_parquet(output_parquet, index=False)
    df_insecta.to_csv(output_csv, index=False)

    print(f"File '{output_parquet.name}' successfully saved!\n")
    print("--- Processing ended successfully! ---")


if __name__ == "__main__":
    extract_ctfb_data()
