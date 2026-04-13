# Git Global Proxy Quick Switch

This repo now includes a one-command Git proxy switch script for VS Code and local terminal use.

Script location:

- `03_Scripts/git_proxy.sh`

## Why this exists

When GitHub is only reachable through a local proxy, `git push` may still fail even if your browser works, because Git CLI does not automatically inherit the proxy used by your GUI app or old VS Code terminals.

This script writes Git global proxy settings directly with `git config --global`, so normal commands like `git pull`, `git fetch`, and `git push` work without adding temporary `-c http.proxy=...` flags every time.

## Default proxy values

- `http.proxy=http://127.0.0.1:7897`
- `https.proxy=http://127.0.0.1:7897`

These defaults match the verified working proxy in the current development environment.

## Commands

Enable Git global proxy:

```bash
bash 03_Scripts/git_proxy.sh on
```

Disable Git global proxy:

```bash
bash 03_Scripts/git_proxy.sh off
```

Show current Git global proxy status:

```bash
bash 03_Scripts/git_proxy.sh status
```

Test whether Git can currently reach GitHub:

```bash
bash 03_Scripts/git_proxy.sh test
```

## Custom proxy ports

If your local proxy port changes, override it inline:

```bash
bash 03_Scripts/git_proxy.sh on \
  --http http://127.0.0.1:7890 \
  --https http://127.0.0.1:7890
```

If you want to test a different remote URL:

```bash
bash 03_Scripts/git_proxy.sh test \
  --remote https://github.com/<owner>/<repo>.git
```

## Recommended workflow in VS Code

When GitHub is unstable:

1. Run `bash 03_Scripts/git_proxy.sh on`
2. Use normal Git commands in VS Code terminal or Source Control
3. Run `bash 03_Scripts/git_proxy.sh off` when you no longer need the proxy

Because the script writes Git global config, even a newly opened VS Code terminal can keep working without repeating the full push command with temporary proxy arguments.

## Current verified behavior

The following path was verified successfully in this workspace:

```bash
git push JATO_Analysis_System main:main
```

after Git global proxy was configured to `http://127.0.0.1:7897`.