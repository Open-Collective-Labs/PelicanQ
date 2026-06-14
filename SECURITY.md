# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in PelicanQ, please report it privately to the maintainers. **Do not** disclose it publicly until it has been addressed.

To report a vulnerability, create a GitHub Security Advisory at:

https://github.com/Open-Collective-Labs/PelicanQ/security/advisories/new

Alternatively, contact the maintainers directly via email (listed in the commit history).

## Scope

The following are in scope:

- The `pelicanqd` daemon binary
- The core engine (`pelicanq-core`)
- Raft integration (`pelicanq-raft`)
- Official SDKs (Rust, Go, Python, Node.js, Java)
- HTTP, gRPC, and MQTT protocol handlers

The following are **out of scope**:

- Third-party dependencies (report to their respective maintainers)
- Issues in example code (those are not production-ready by design)

## Response Timeline

We aim to:

- Acknowledge receipt within 48 hours.
- Assess and triage within 5 business days.
- Provide a fix or mitigation timeline within 10 business days.
- Release a security advisory publicly once the fix is deployed.
