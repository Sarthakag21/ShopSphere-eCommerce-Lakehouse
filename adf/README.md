# Azure Data Factory

This folder contains sanitized, human-readable project definitions for the ADF layer.

## Pipelines

- `PL_INITIAL_MYSQL_TO_ADLS.json` — initial full-load pattern.
- `PL_METADATA_INCREMENTAL_MYSQL_TO_ADLS.json` — metadata-driven incremental pattern.

## Datasets

Dynamic MySQL and ADLS dataset definitions are represented in `datasets/`.

## Linked services

`linked-services/` contains sanitized connection definitions. Secrets are deliberately omitted.

### Live export note
These files document the working design used in the project; they are not raw Azure exports with credentials. Export current ADF JSON from the live factory before using these definitions for environment recreation.
