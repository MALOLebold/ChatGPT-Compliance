# ChatGPT-Compliance

Barebones Python CLI for exporting ChatGPT Enterprise Compliance Logs from `api.chatgpt.com`, plus a local compliance scanner for exported prompts.

The exporter writes:

- `raw.jsonl`: downloaded Compliance Log records
- `prompts.jsonl`: extracted user/human prompt records with source JSON attached
- `manifest.json`: run metadata, counts, and extraction warnings

GPT/assistant response message records are filtered out of the saved output.

## Complete Workflow

From the project folder:

```powershell
cd C:\Users\Username\Projects\GPT_Compliance   # This is an example path, but you would input wherever you store this project.
```

Set the Compliance API key for the current PowerShell session:

```powershell
$env:COMPLIANCE_API_KEY = "<key>"
```

Run a 30-day workspace export:

```powershell
py -m gpt_compliance_exporter export `
  --principal-id "<workspace_id>" `
  --event-type "CONVERSATION_MESSAGE" `
  --out-dir exports `
  --days 30 `
  --limit 100 (Optional, but I would not use this)
```

Organization IDs starting with `org-` are routed to organization-scope Compliance Logs. Other IDs are routed to workspace-scope logs.

After exporting `CONVERSATION_MESSAGE` logs, scan user prompts against the local M+N AI policy rules and OpenAI Usage Policy rules:

```powershell
py -m compliance_script.scan_prompt_compliance `
  --input exports/raw.jsonl `
  --out-dir compliance_script/output
```

The scanner skips GPT/assistant responses, classifies every reviewed prompt with a score and risk level, and writes the full prompt text by default. Add `--redact-prompt` if a review workflow should store redacted excerpts instead.

## Notes

- Use the ChatGPT workspace ID for the first ChatGPT Enterprise prompt export.
- The key is read only from `COMPLIANCE_API_KEY` and is never written to output files.
- Exported prompt data and scanner outputs are ignored by Git via `.gitignore`.

Scanner outputs:

- `compliance_findings.xlsx`: Excel workbook with Reviewed Prompts, Flagged Prompts, Summary by User, Summary by Category, and Summary by Risk Level sheets
- `compliance_reviewed_prompts.jsonl`: all reviewed prompt classifications
- `compliance_findings.jsonl`: flagged prompt classifications only
- `compliance_summary.json`: scan counts and summary totals

## Tests

```powershell
py -m unittest discover
```
