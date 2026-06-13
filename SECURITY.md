# Security

Do not commit shop feed URLs, API tokens, AWS credentials, inventory IDs,
warehouse IDs, passwords, account IDs, customer names, or exported catalog
data.

Use GitHub Actions secrets or a local ignored `.env` file. The repository
contains `.env.example` with placeholders only.

If a secret is committed accidentally:

1. Revoke or rotate it immediately.
2. Remove it from Git history before making the repository public.
3. Re-run the repository safety checks.

Please report security issues privately to the repository owner.
