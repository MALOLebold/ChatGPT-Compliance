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
  --limit 100 # (Optional, but I would not use this)
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

## Automated Cloud Power BI Workflow

The pipeline command runs the export, scans the prompts, writes the same scanner outputs, uploads `compliance_findings.xlsx` directly to SharePoint with Microsoft Graph, and optionally triggers a Power BI semantic model refresh:

```powershell
$env:COMPLIANCE_API_KEY = "<compliance_api_key>"
$env:MICROSOFT_TENANT_ID = "<tenant_id>"
$env:MICROSOFT_CLIENT_ID = "<entra_app_client_id>"
$env:MICROSOFT_CLIENT_SECRET = "<entra_app_client_secret>"

py -m compliance_script.run_pipeline `
  --principal-id "<workspace_id>" `
  --event-type "CONVERSATION_MESSAGE" `
  --export-dir exports `
  --scan-out-dir compliance_script/output `
  --sharepoint-site-url "https://<tenant>.sharepoint.com/sites/<site_name>" `
  --sharepoint-drive-name "Documents" `
  --sharepoint-folder "ChatGPT Compliance" `
  --sharepoint-filename "compliance_findings.xlsx" `
  --powerbi-workspace-id "<powerbi_workspace_id>" `
  --powerbi-dataset-id "<powerbi_dataset_id>" `
  --days 30 `
  --limit 100
```

`COMPLIANCE_API_KEY` and Microsoft credentials should come from environment variables or Azure Key Vault. Do not put secrets in commands, scripts, PBIX files, or GitHub.

The pipeline exits successfully even when prompts are flagged. A non-zero exit means the export, scan, SharePoint upload, or Power BI refresh failed.

Power BI refresh is optional. Omit `--powerbi-workspace-id` and `--powerbi-dataset-id` if you only want to upload the workbook to SharePoint.

### Required Microsoft Setup

- Entra app registration with a client secret or managed identity-backed equivalent.
- Microsoft Graph permission to write to the target SharePoint document library, such as `Sites.ReadWrite.All` or a narrower site-scoped permission approved by IT.
- Power BI API permission `Dataset.ReadWrite.All` if using automatic refresh.
- Power BI tenant setting that allows service principals to use Power BI APIs.
- The Power BI report/semantic model should read from the stable SharePoint workbook path.

### Azure Automation Notes

- Store `COMPLIANCE_API_KEY`, `MICROSOFT_TENANT_ID`, `MICROSOFT_CLIENT_ID`, and `MICROSOFT_CLIENT_SECRET` in Azure Key Vault or Automation variables/secrets.
- Schedule the runbook monthly or weekly.
- Keep Azure Storage for optional archive copies if needed, but the live Power BI workbook is uploaded to SharePoint.

### Power BI Setup

- Store `compliance_findings.xlsx` in a restricted SharePoint folder.
- In Power BI Desktop, connect to the SharePoint file URL instead of a local `C:\...` path.
- Publish the PBIX to Power BI Service after the report points to the cloud workbook.
- Prefer the pipeline's Power BI refresh trigger after a successful SharePoint upload. A scheduled Power BI refresh can remain as a fallback.
- Do not commit live `.pbix` files unless they are confirmed sanitized; they may contain cached prompt data.

## Tests

```powershell
py -m unittest discover
```
