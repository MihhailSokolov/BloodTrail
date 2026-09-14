# Security policy

BloodTrail patches a security product's deployment, so vulnerability reports get
priority attention.

## Reporting a vulnerability

Please report vulnerabilities privately through GitHub's security advisory flow:
**[Security → Report a vulnerability](https://github.com/MihhailSokolov/BloodTrail/security/advisories/new)**
on this repository. Do not open a public issue for anything you believe is
exploitable.

You can expect an acknowledgement within a week. Please include the BloodTrail
release (or commit), the upstream BloodHound tag you deployed against, and enough
detail to reproduce.

## Supported versions

Only the latest release receives fixes. The installer's own attack surface --
what it changes on an operator's host, what it backs up, and how rollback works
-- is documented in the [README](README.md#installing-on-an-existing-bloodhound-ce-deployment).

## Scope notes

- BloodTrail inherits BloodHound CE's own security model; vulnerabilities in
  unpatched upstream code should go to [SpecterOps](https://github.com/SpecterOps/BloodHound/security).
- The `install.sh` bootstrap verifies the CLI archive against the release's
  `checksums.txt` before running it. The script itself is served from the same
  GitHub release; if your threat model requires it, download and inspect it
  instead of piping to `sh`, and pin `BLOODTRAIL_VERSION` to a release you have
  audited.
