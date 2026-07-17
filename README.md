# rna-seq-pipeline

Downloads RNA-seq experiment metadata from NCBI GEO and maps tissue, cell type, and strain descriptions to RGD ontology terms.

## What it does

**Phase 1 -- Download:**
- Downloads GEO SOFT files over HTTP from `https://ftp.ncbi.nlm.nih.gov/geo/series/`, one series at a time (single-threaded, to be gentle on NCBI)
- Parses SOFT format and inserts series/sample metadata into the `RNA_SEQ` table

**Phase 2 -- Mapping:**
- Matches sample tissue descriptions to UBERON ontology terms
- Matches sample cell type descriptions to CL ontology terms
- Matches sample strain descriptions to RS ontology terms and RGD strain IDs
- Uses exact matching against ontology terms and synonyms, then lemmatized matching for unmatched records
- Updates `RNA_SEQ` table with mapped ontology accessions

## Usage

```
java -jar rna-seq-pipeline.jar [--gse GSE53960]
```

- No arguments -- full run: scans every GEO grouping folder up to the newest one (discovered from the
  live root listing at runtime), then maps all pending rows created after the cutoff date.
- `--gse <accession>` -- targeted single-series run: loads only that one series (its grouping folder is
  derived from the accession, e.g. `GSE53960` -> `GSE53nnn/`), then maps only that series' pending rows.
  No full folder scan and no global remap. Handy for loading a newly published series on demand.

Download and mapping phases can be independently enabled/disabled in `AppConfigure.xml`; `--gse` always
loads the named series, and honors `performMapping` for the subsequent map step.
