# Contributing to InsectAPI

First off, thank you for considering contributing to InsectAPI! It's people like you that make open-source tools possible.

This document provides guidelines and instructions for contributing to this repository.

## Table of Contents
1. [Code of Conduct](#code-of-conduct)
2. [How Can I Contribute?](#how-can-i-contribute)
3. [Development Setup](#development-setup)
4. [Pull Request Process](#pull-request-process)
5. [Coding Style](#coding-style)


## Code of Conduct
By participating in this project, you are expected to uphold a welcoming and professional environment. Please be respectful to all contributors and maintainers. You can read more about it at our [Code of Conduct file](https://github.com/Caio-Freitas005/insectapi?tab=coc-ov-file).


## How Can I Contribute?
* **Reporting Bugs:** Open an issue describing the bug, how to reproduce it, and the expected behavior.
* **Suggesting Enhancements:** Have an idea for a new endpoint or data feature? Open an issue to discuss it before writing any code.
* **Code Contributions:** Look for open issues tagged with `good first issue` or `help wanted`.


## Development Setup

We use modern Python tooling to keep the environment fast and reproducible.

1. **Fork and Clone:** 

Fork the repository to your GitHub account and clone it locally.
```bash
git clone https://github.com/Caio-Freitas005/insectapi.git
cd insectapi
```

2. **Install `uv`**

We use uv for dependency management. If you don't have it, install it. 
[uv docs](https://docs.astral.sh/uv/getting-started/installation/)

3. **Sync Dependencies** 

Create the virtual environment and install dependencies.
```bash
uv sync
```

4. **Run the API locally**
```bash
# --reload is used to auto-reload the server when changing code
uv run uvicorn api.main:app --reload
```


## Pull Request Process

1. **Branch Naming**: Create a new branch for your feature or bugfix. Use descriptive names like feature/search-pagination or bugfix/fix-skip-limit.

2. **Commit Messages**: Write clear and concise commit messages explaining what and why you changed. We adopt mainly [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) with scopes.

3. **Update Documentation**: If you add a new endpoint or change existing behavior, please update the README.md and docstrings accordingly.

4. **Submit PR**: Open a Pull Request against the main branch. Provide a clear description of the changes and link any related open issues.

## Coding Style

1. **Formatting**: We strictly follow standard Python conventions (PEP 8), so linters as `flake8` or `ruff` are recommended.

2. **Type Hints**: Since we use FastAPI, ensure all your functions, parameters, and return types have proper Python type hints.

3. **Docstrings**: Write docstrings for all new functions and API endpoints to keep the automatic Swagger documentation (`/docs`) clean and informative.
   