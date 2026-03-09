# cases-parser

`cases-parser` exports hearing data from the Latvian court calendar into a single Excel workbook. It crawls the monthly summary index, follows each non-empty process link, normalizes civil, criminal, and administrative rows into one schema, and enriches criminal rows with cited law text and a short case summary.

## What It Does

- crawls monthly hearing indexes such as `https://tis.ta.gov.lv/court.jm.gov.lv/stat/html/index_202603.html`
- follows linked cells across:
  - `Civilprocess`
  - `Kriminālprocess`
  - `Administratīvais process`
  - `Administratīvais pārkāpuma process`
  - `Administratīvais pārkāpuma process pēc 01.07.2020`
- decodes court pages as `windows-1257`
- writes one workbook with normalized shared columns plus process-specific details
- resolves criminal-law citations against `likumi.lv`
- generates a short summary for criminal rows using a local MLX helper

## Setup

Install Node dependencies:

```bash
npm install
```

Optional: install the local MLX runtime used for criminal-row summaries:

```bash
python3 -m pip install "mlx-lm>=0.30.7"
```

## Usage

Run the parser with a monthly index URL:

```bash
node scripts/merge-kriminalprocess.mjs <monthly-index-url> [output-path]
```

Example:

```bash
node scripts/merge-kriminalprocess.mjs 'https://tis.ta.gov.lv/court.jm.gov.lv/stat/html/index_202603.html'
```

Default output file:

```text
court-calendar-<index-name>.xlsx
```

## Output

The workbook includes:

- `Procesa grupa`
- `City`
- `Sēdes datums`
- `Sēdes laiks`
- `Sēdes veids`
- `Procesa veids`
- `Court`
- `Pirmā puse / pieteicējs / prasītājs / apsūdzētais`
- `Otrā puse / atbildētājs`
- `Lietas būtība / prasījums`
- `Lietas rakstura kopsavilkums`
- `Lietas numurs`
- `Arhīva numurs`
- `Apsūdzības panti (deciphered)`
- `Citēto normu teksts`
- `Instance`
- `Tiesnesis`
- `Tiesas sēdes laiks`
- `Seriousness rank (1-5)`
- `Source`

Criminal-only enrichment is populated only for `Kriminālprocess` rows:

- `Apsūdzības panti (deciphered)`
- `Citēto normu teksts`
- `Lietas rakstura kopsavilkums`
- `Seriousness rank (1-5)`

## Project Files

- `scripts/merge-kriminalprocess.mjs`: main parser and workbook writer
- `scripts/mlx_summarize.py`: local summary helper
- `data/law-sources.json`: law-code registry for citation resolution
- `run-kriminalprocess.sh`: convenience wrapper for the current month

## Current Limitations

- some citations still do not resolve to exact official text and are emitted as visible placeholders
- criminal-row summaries fall back to a deterministic summary when the local MLX result is weak
- law lookup uses the current consolidated text from `likumi.lv`, not historical text at the hearing date
