# Releasing

A release is a git tag, `vX.Y.Z`, and a GitHub release holding a source
tarball and its SHA-256. The release workflow makes both. Nothing is tagged or
uploaded by hand.

## Cutting a release

1. In a pull request, set the version in every place that declares one:
   - `MODULE.bazel`: `module(name = "protovcs", version = "X.Y.Z")`
   - `rust/Cargo.toml`: `[workspace.package] version = "X.Y.Z"`
   - `rust/MODULE.bazel`: `module(name = "protovcs_rust", version = "X.Y.Z")`
2. Merge it.
3. On the **Actions** tab, run the **Release** workflow on `main`, with the
   version, such as `0.0.1`.

The workflow then:
1. refuses a version that any of those files disagree with, or one already
   tagged;
2. builds the tarball twice and refuses if the two differ;
3. tags the commit it ran on as `vX.Y.Z`;
4. publishes the release with `protovcs-X.Y.Z.tar.gz` and
   `protovcs-X.Y.Z.tar.gz.sha256`, and notes generated from the pull requests
   since the last release.

## The tarball

GitHub's own "Source code" archives are not guaranteed to keep the same bytes,
so a build that pins one by hash can break without the source changing. The
tarball attached to a release is built by
[`internal/cmd/releasetar`](internal/cmd/releasetar) instead. It reads the
commit's git tree rather than the working directory, and:

- sorts entries by path, with each directory before its contents;
- sets every modification time to the commit's time, and every owner to zero;
- writes no name or time into the gzip header.

The bytes depend only on the commit, the version, and the Go version that
builds `releasetar`, which the workflow pins. To check a release, build its
tarball again from the tag, with that Go version, and compare checksums:

```bash
git checkout vX.Y.Z
```

```bash
go run ./internal/cmd/releasetar -version X.Y.Z -out /tmp/check
```

```bash
cat /tmp/check/protovcs-X.Y.Z.tar.gz.sha256
```

Every path in the tarball is under `protovcs-X.Y.Z/`. With Bazel, it can be
used with `archive_override` and its `integrity`, or `http_archive` and its
`sha256`, with `strip_prefix = "protovcs-X.Y.Z"`.
