# GPT Compliance Exporter

Barebones Python CLI for exporting ChatGPT Enterprise Compliance Logs from `api.chatgpt.com`.

The exporter writes:

- `raw.jsonl`: downloaded Compliance Log records
- `prompts.jsonl`: extracted user/human prompt records with source JSON attached
- `manifest.json`: run metadata, counts, and extraction warnings

## Usage

Set the Compliance API key:

```powershell
$env:COMPLIANCE_API_KEY = "<key>"
```

Run a 30-day workspace export:

```powershell
py -m gpt_compliance_exporter export `
  --principal-id "<workspace_id>" `
  --event-type "<conversation_event_type>" `
  --out-dir exports `
  --days 30 `
  --limit 100
```

Organization IDs starting with `org-` are routed to organization-scope Compliance Logs. Other IDs are routed to workspace-scope logs.

## Notes

- Use the ChatGPT workspace ID for the first ChatGPT Enterprise prompt export.
- The exact conversation-message `event_type` must come from your Enterprise Compliance API access/docs.
- The key is read only from `COMPLIANCE_API_KEY` and is never written to output files.

## Tests

```powershell
py -m unittest discover
```
