# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, not in public issues: use
GitHub's "Report a vulnerability" button on the repository's Security tab
(private vulnerability reporting). Include what is affected, how to
reproduce it, and the impact you see.

We aim to acknowledge reports within 3 working days, agree on a fix and a
disclosure date within 14 days, and credit reporters who want it.

## Supported versions

Security fixes go to the latest release. Before 1.0, upgrade to the
latest release to get them.

## What we do

The [threat model](docs/threat-model.md) lists what Pumasi defends
against and what it does not. Every pull request runs `cargo deny`,
`cargo audit`, CodeQL, the fuzz targets in `fuzz/` and an SSO test
against real Keycloak and OpenLDAP; every week, longer fuzzing and
5-hour soak tests. Release binaries come with `SHA256SUMS`, which the
install scripts check.

Pumasi has not had an independent third-party security audit. The
maintainers review the code themselves before releases
([docs/security-review.md](docs/security-review.md) has the scope).
