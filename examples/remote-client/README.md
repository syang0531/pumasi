# pumasi-remote

Runs jobs on a Pumasi cluster through its HTTP API (`pumasi ui
--api-only`), from a machine without Pumasi: a laptop, a CI job, a
Claude Code cloud session behind an HTTPS proxy. Bash, curl and jq;
python3 3.11 or later to send only the files a job names.

```bash
export PUMASI_URL=https://pumasi.example.com
export PUMASI_TICKET=pumasi...        # unset when a proxy adds the header
pumasi-remote status                  # can the cluster take my work now?
pumasi-remote run ../python-wordcount # submit, wait, unpack to pumasi-out/<id>
```

| Command | Does |
| --- | --- |
| `whoami` | the ticket's name, role and account, and the account's job policy, quota and use |
| `status` | nodes that can take this account's work now, and why the others cannot |
| `jobs` | this account's jobs |
| `submit DIR\|JOB.toml [--account A] [--priority N] [--after ID]` | sends the job file and the files it names, prints the job id |
| `check DIR\|JOB.toml [submit options]` | asks whether the cluster would take the job (role, policy, quotas, files) without submitting it; exit 0 if it would, else the reason (Pumasi 1.4 or later) |
| `wait JOB [--timeout 30m]` | follows it with long polls; exit 0 if it succeeded, 1 if not, 2 on timeout |
| `logs JOB [TASK]` | stdout (and stderr on stderr) of one task |
| `outputs JOB [DEST]` | downloads `outputs.zip` and unpacks it (default `pumasi-out/JOB`) |
| `cancel JOB` | cancels it |
| `run DIR\|JOB.toml [DEST] [--timeout 30m]` | all of the above in one go, printing failed tasks' logs |

Setting up the server, tickets, policies, Cloudflare Tunnel and Claude
Code network secrets: [docs/api.md](../../docs/api.md), sections "Put it
on the internet" and "Clients behind an HTTPS proxy". CI runs this script
against a real cluster with the rest of that page.
