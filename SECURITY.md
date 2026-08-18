# Security Policy

## Supported Surface

This repository is a sanitized research snapshot. It is not a hosted trading
service and it does not include production credentials or data.

## Reporting a Vulnerability

Do not publish credentials, exploit details, private infrastructure data, or
production evidence in a public issue.

Use GitHub private vulnerability reporting or contact the repository owner
through the GitHub profile before sharing sensitive details.

## Deployment Requirements

- Keep all secrets in an untracked environment file or dedicated secret store.
- Restrict permissions on environment and backup files.
- Put Watchtower behind an authenticated reverse proxy.
- Restrict CORS and trusted proxy headers for any Internet-facing deployment.
- Do not expose DuckDB, Ollama, Qdrant, backup repositories, or daemon control
  endpoints to the public Internet.
- Keep Shadow disconnected from broker execution.
- Treat scraped web content as untrusted until review and provenance checks.
- Rotate a credential immediately if it is ever committed or logged.

## Public Snapshot Boundary

The public branch excludes real credentials, private addresses, databases,
logs, reports, vector stores, portfolio files, and private Git history.
