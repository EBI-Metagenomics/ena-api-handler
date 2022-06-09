[![Testing](https://github.com/EBI-Metagenomics/ena-api-handler/actions/workflows/test.yml/badge.svg)](https://github.com/EBI-Metagenomics/ena-api-handler/actions/workflows/test.yml)

# ENA API handler

This repository contains an ENA API client, mainly used by the EMBL-EBI MGnify team for the internal automation system.

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
 * ENA_API_USER: The username for the the ENA search Portal
 * ENA_API_PASSWORD: The password for the ENA search Portal
