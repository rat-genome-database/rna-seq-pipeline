# rna-seq-pipeline

Downloads RNA-seq experiment metadata from NCBI GEO and maps tissue, cell type, and strain descriptions to RGD ontology terms.

## What it does

**Phase 1 -- Download:**
- Downloads GEO SOFT files via multi-threaded FTP from `ftp://ftp.ncbi.nlm.nih.gov/geo/series/`
- Parses SOFT format and inserts series/sample metadata into the `RNA_SEQ` table

**Phase 2 -- Mapping:**
- Matches sample tissue descriptions to UBERON ontology terms
- Matches sample cell type descriptions to CL ontology terms
- Matches sample strain descriptions to RS ontology terms and RGD strain IDs
- Uses exact matching against ontology terms and synonyms, then lemmatized matching for unmatched records
- Updates `RNA_SEQ` table with mapped ontology accessions

## Usage

```
java -jar rna-seq-pipeline.jar [--start N] [--stop M]
```

- `--start N` -- first GEO folder index to download (default 0)
- `--stop M` -- last GEO folder index to download (default 327)

Download and mapping phases can be independently enabled/disabled in `AppConfigure.xml`.
