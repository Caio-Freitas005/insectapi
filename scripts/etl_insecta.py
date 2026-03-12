from pathlib import Path
from typing import Any

import pandas as pd

# Global Constant for translations
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


def process_taxons(data_dir: Path) -> list[str]:
    """Extracts, cleans, and translates taxonomic data for the Insecta class."""
    taxon_file = data_dir / "taxon.txt"
    output_parquet = data_dir / "insects_taxons.parquet"
    output_csv = data_dir / "insects_taxons.csv"

    if not taxon_file.exists():
        print(f"Error: The file '{taxon_file}' was not found.")
        return []

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
        return []

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

    df_insecta.to_parquet(output_parquet, index=False)
    df_insecta.to_csv(output_csv, index=False)
    print(
        "Files 'insects_taxons.parquet' and 'insects_taxons.csv' successfully saved!\n"
    )

    # Return the extracted IDs to be used by the vernacular names function
    return df_insecta["id"].tolist()


def process_vernacular_names(data_dir: Path, insects_ids: list[str]) -> None:
    """Extracts and cleans vernacular names matching the provided insect IDs."""
    if not insects_ids:
        print("No insect IDs provided. Skipping vernacular names processing.")
        return

    vernacular_file = data_dir / "vernacularname.txt"
    output_parquet = data_dir / "insects_vernacular_names.parquet"
    output_csv = data_dir / "insects_vernacular_names.csv"

    if not vernacular_file.exists():
        print(f"Error: The file '{vernacular_file}' was not found. Skipping.")
        return

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
        return

    print("Extracting and cleaning vernacular names for insects...")
    df_vernacular_insecta = (
        df_vernacular[df_vernacular["id"].isin(insects_ids)]
        .copy()
        .reset_index(drop=True)
    )

    # Cleaning and type casting
    df_vernacular_insecta = df_vernacular_insecta.fillna("").astype(str)

    print(f"Total vernacular names found: {len(df_vernacular_insecta)}")

    df_vernacular_insecta.to_parquet(output_parquet, index=False)
    df_vernacular_insecta.to_csv(output_csv, index=False)
    print(
        "Files 'insects_vernacular_names.parquet' and 'insects_vernacular_names.csv' successfully saved!\n"
    )


def extract_ctfb_data() -> None:
    """Main orchestrator function for the ETL process."""
    print("--- Starting Data Processing (InsectAPI) ---\n")

    data_dir = Path("datasets")

    # Step 1: Process Taxons and get the list of valid insect IDs
    insects_ids = process_taxons(data_dir)

    # Step 2: Process Vernacular Names using the IDs from Step 1
    process_vernacular_names(data_dir, insects_ids)

    print("--- Processing ended successfully! ---")


if __name__ == "__main__":
    extract_ctfb_data()
