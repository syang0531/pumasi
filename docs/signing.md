# Signing the Windows binaries

Windows 11's Smart App Control blocks unsigned programs that Microsoft's
cloud service does not know yet, and SmartScreen warns about them. Pumasi's
Windows binaries get an Authenticode signature from
[SignPath Foundation](https://signpath.org), which signs open source
projects for free with a certificate issued to "SignPath Foundation". The
key stays in SignPath's HSM; the release workflow only sends it the
binaries GitHub Actions built from this repository, and a person approves
each release in SignPath.

Until the setup below is done, `release.yml` skips the `sign` job and
releases ship unsigned, as before. See "Windows Smart App Control" in the
README for what that means on a machine where it is on.

## One-time setup

1. **Apply** at <https://signpath.org/apply> as the repository owner. The
   project qualifies on the points SignPath checks: Apache-2.0 for all of
   its own code, no proprietary components, released and documented, built
   from source in GitHub Actions on GitHub-hosted runners, and a
   "Code signing policy" section on the home page (in the README).
2. **Turn on multi-factor authentication** for every team member, on GitHub
   and on SignPath. SignPath requires it.
3. **In SignPath**, once the application is accepted:
   - Add the predefined trusted build system **GitHub.com** to the
     organization and link it to the project. Installing the SignPath
     GitHub App on this repository is optional (it lets SignPath check
     pipeline policies).
   - Create the project with the slug `pumasi` and a signing policy with
     the slug `release-signing` that uses the SignPath Foundation
     certificate and requires approval. (Other slugs work too; set them as
     the repository variables `SIGNPATH_PROJECT_SLUG` and
     `SIGNPATH_POLICY_SLUG`.)
   - Set the project's default artifact configuration to the one below. The
     workflow uploads the two Windows binaries as one GitHub artifact,
     which GitHub stores as a ZIP file.
   - Create an API token for a user with the submitter role on that policy.
4. **In this repository's settings** (Settings > Secrets and variables >
   Actions):
   - variable `SIGNPATH_ORGANIZATION_ID`: the organization id shown in
     SignPath;
   - secret `SIGNPATH_API_TOKEN`: the token from step 3.

The next release tag then signs the Windows binaries before publishing.

```xml
<?xml version="1.0" encoding="utf-8"?>
<artifact-configuration xmlns="http://signpath.io/artifact-configuration/v1">
  <zip-file>
    <pe-file path="pumasi-windows-x86_64.exe" product-name="Pumasi" max-matches="1">
      <authenticode-sign/>
    </pe-file>
    <pe-file path="pumasi-windows-aarch64.exe" product-name="Pumasi" max-matches="1">
      <authenticode-sign/>
    </pe-file>
  </zip-file>
</artifact-configuration>
```

`product-name` is checked against the version information that
`crates/cli/build.rs` writes into every Windows build (product name
"Pumasi", product and file version from `Cargo.toml`); the release
workflow checks that it is there.

## Each release

1. Push the tag as before. The `sign` job uploads the Windows binaries and
   sends a signing request to SignPath.
2. An approver approves the request in SignPath (the job log links to it).
   The job waits up to about 5 hours, then fails, and the release is not
   published; run the workflow again to send a new request.
3. The job checks that both binaries carry a SignPath Foundation signature,
   and `publish` releases them in place of the unsigned ones. The
   checksums, attestations and `pumasi upgrade rollout` all use the signed
   files.
