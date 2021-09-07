# ena-api-handler

This repository contains internal tools and libs used by the MGnify team at EMBL-EBI.

## Installation

```bash
pip install -U git+git://github.com/EBI-Metagenomics/ena-api-handler.git
```
## Development

Create python virtual env or a conda env.

Install the dev dependencies:

```bash
pip install -r requirements-dev.txt
```

If you haven't configured the [pre-commit hooks](https://pre-commit.com/), please do so:

```bash
pre-commit-hooks install
```

## Setting up analysis_request_cli

The following environment vars must be defined:
 * ENA_API_USER: MGnify username for the ENA search Portal
 * ENA_API_PASSWORD: MGnify username for the ENA search Portal
