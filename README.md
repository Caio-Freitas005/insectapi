# InsectAPI

![Version](https://img.shields.io/badge/version-0.2.1-blue.svg)
![Python](https://img.shields.io/badge/python-3.13-yellow.svg)
![FastAPI](https://img.shields.io/badge/framework-FastAPI-009688.svg)
![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)

API to access brazilian Insecta taxonomic data. This project provides a streamlined, RESTful interface to query and search for insect taxonomic information using a modern Python stack.

## Data Source 

The biological and taxonomic data used in this project are in the public domain and come from the **Catálogo Taxonômico da Fauna do Brasil (CTFB)**. 

**Official Citation:** 
> Brazilian Zoology G (2026). Catálogo Taxonômico da Fauna do Brasil. Version 1.48. Instituto de Pesquisas Jardim Botanico do Rio de Janeiro. Checklist dataset https://doi.org/10.15468/c4cauy accessed via GBIF.org on 2026-03-11.

*Note: This repository does not host or distribute the original database. The code here acts as an ETL (Extract, Transform, Load) tool to facilitate the consumption of this data by other applications.*


## Features

* **Read-Only API:** Safe and fast access to taxonomic records.
* **Search Capabilities:** Query insects by scientific name or vernacular (common) name.
* **Pagination:** Built-in pagination for handling large datasets efficiently.
* **Automated ETL:** A GitHub Actions workflow automatically extracts, transforms, and loads the latest dataset to Hugging Face on a monthly schedule.
* **Cloud Integration:** The API automatically downloads the `.parquet` dataset via the Hugging Face SDK upon startup if not found locally.


## Tech Stack

* **Language:** Python 3.13
* **Framework:** FastAPI
* **Data Processing:** Pandas (Parquet format)
* **Package Manager:** uv
* **Infrastructure:** Docker, Hugging Face Hub, GitHub Actions, Render (deploy)


## Getting Started

### Prerequisites

To run this project locally, ensure you have the following installed:
* [Python 3.13+](https://www.python.org/downloads/)
* [uv](https://github.com/astral-sh/uv) (Extremely fast Python package installer and resolver)

### Local Installation

1. Clone the repository:
```bash
git clone https://github.com/Caio-Freitas005/insectapi.git
cd insectapi
```

2. Sync dependencies using `uv`
```bash
uv sync
```

3. Run the development server
```bash
# --reload is used to auto-reload the server when changing code
uv run uvicorn api.main:app --reload
```
*The API will be available at `http://127.0.0.1:8000`*

*The interactive Swagger UI documentation can be accessed at `http://127.0.0.1:8000/docs`.*

### Running with Docker
If you prefer to use containers, you can build and run the provided Docker image:

1. Build the image:
```bash
docker build -t insectapi .
```

2. Run the container:
```bash
docker run -p 8000:8000 insectapi
```

### API Endpoints

| Method | Endpoint         | Description                                                                 |
|--------|------------------|-----------------------------------------------------------------------------|
| GET    | /                | Health check and API status.                                                |
| GET    | /insects/        | Returns a paginated list of insects. Accepts skip and limit parameters.     |
| GET    | /insects/search  | Searches insects by scientific or vernacular name. Requires a q parameter (min 3 characters). |

### ETL 
There is an ETL script provided at `scripts/`

You can use it or change as you like, but for simpler use as it is provided, you can run in two ways:

1. Run in debug mode (locally)
```bash
uv run python scripts/etl_insecta.py --debug
```

2. Run in normal mode
```bash
uv run python scripts/etl_insecta.py
```
The difference here is that normal mode upload the resulting `.parquet` file to a dataset at Hugging Face.


## Contributing

Contributions from the community are welcome! Whether it is a bug report, a new feature, or a documentation correction, please read [CONTRIBUTING.md](https://github.com/Caio-Freitas005/insectapi?tab=contributing-ov-file) guide to understand how to submit a Pull Request and the coding standards we follow.


## License

This project is licensed under the terms found in the [LICENSE](https://github.com/Caio-Freitas005/insectapi?tab=MIT-1-ov-file) file.