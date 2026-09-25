# Releasing Ricochet Server

A release is a GitHub release of `stephanfeb/go-ricochet` holding:

| File | Built by |
|---|---|
| `ricochet-server_X.Y.Z_{amd64,arm64}.deb` | the release workflow |
| `ricochet_X.Y.Z_linux_{amd64,arm64}.tar.gz` | the release workflow |
| `ricochet_X.Y.Z_windows_amd64.zip` | the release workflow |
| `ricochet_X.Y.Z_darwin_{arm64,amd64}.dmg` | `scripts/sign-macos-release.sh`, on your Mac, from the tarballs the workflow built |
| `SHA256SUMS` | the workflow, rewritten by the script |

The workflow builds and tests everything from the tag, then leaves the release as a draft. The macOS disk images are made on a Mac that holds the Developer ID certificate, because signing secrets are never stored on GitHub. An unsigned macOS binary is killed by macOS when a user downloads and runs it, so the draft is not published until the script has run. The script publishes it.

## One-time setup on the Mac

1. **Xcode command-line tools**, for `codesign`, `xcrun notarytool` and `xcrun stapler`. Check with `xcode-select -p`.
2. **Rosetta**, so the script can run the amd64 binary to check it: `softwareupdate --install-rosetta`.
3. **The Developer ID Application certificate in the login keychain:**

   ```
   security find-identity -v -p codesigning
   ```

   The list must include `Developer ID Application: Werkswinkel Pte Ltd (32XLPKQ5TF)`. To sign with another identity, set `SIGN_IDENTITY` to its name.
4. **A notarytool keychain profile.** This Mac has `cloak-notary`, shared with cloak-cli and pool-coordinator. To make one:

   ```
   xcrun notarytool store-credentials cloak-notary --apple-id <Apple ID> --team-id 32XLPKQ5TF
   ```

   Check it with `xcrun notarytool history --keychain-profile cloak-notary`.
5. **The GitHub CLI, logged in with `repo` scope** (`gh auth status`).

## Making a release

### 1. Check CI on `main` is green

The release workflow runs the same suite first and makes no release if it fails.

### 2. Tag and push

```
git tag -a vX.Y.Z -m "Ricochet Server vX.Y.Z"
git push origin vX.Y.Z
gh run watch
```

A tag with a pre-release part (`v1.1.0-rc1`) makes a pre-release; its Debian package version is `1.1.0~rc1`, which dpkg orders before `1.1.0`.

When the run succeeds there is a **draft** release `vX.Y.Z`, which only the repository's maintainers can see. If the run fails, nothing is attached. Fix the fault on `main`, then move the tag. This is safe only while no release has been published from it:

```
git tag -d vX.Y.Z && git push origin :refs/tags/vX.Y.Z
git tag -a vX.Y.Z -m "Ricochet Server vX.Y.Z" && git push origin vX.Y.Z
```

### 3. Sign the macOS builds and publish (on the Mac)

From a checkout of this repository:

```
NOTARY_PROFILE=cloak-notary scripts/sign-macos-release.sh vX.Y.Z
```

For each of arm64 and amd64 the script:

1. **Downloads the tarball** from the release and checks it against `SHA256SUMS`.
2. **Signs the binary** with the Developer ID, under the hardened runtime, with a secure timestamp, and runs `ricochet --version`. The amd64 build runs under Rosetta. Go needs no entitlements.
3. **Packs the directory** into `ricochet_X.Y.Z_darwin_<arch>.dmg` and signs the image.
4. **Notarizes the image** with Apple, staples the ticket to it and validates it. A stapled image carries its own ticket, so the program runs even on a Mac that cannot reach Apple. A bare binary cannot carry a ticket.
5. **Checks the image as a downloader gets it.** It marks a copy with the quarantine attribute a browser sets, mounts it, and checks that it holds exactly the tarball's files. Then it copies the directory out and runs `--version` from the quarantined copy.

Then it uploads the images, rewrites `SHA256SUMS` (the other lines unchanged), deletes the tarballs, and publishes the draft. If anything fails before the upload, the release is untouched; fix the cause and run it again. The script also works on a release that is already published, replacing the files in place.

### 4. Check the published release

In an empty directory:

```
gh release download vX.Y.Z --repo stephanfeb/go-ricochet
shasum -a 256 -c SHA256SUMS
xcrun stapler validate ricochet_X.Y.Z_darwin_arm64.dmg
```

## When something goes wrong

- **Notarization is not accepted.** The script prints Apple's log for the submission. Fetch it again with `xcrun notarytool log <submission id> --keychain-profile cloak-notary`.
- **The quarantined program is killed (exit 137).** Run `codesign -dvvv <file>` and look for `flags=0x10000(runtime)` and a `Timestamp=`. The reason macOS gives is in `/usr/bin/log show --last 5m --predicate 'process == "syspolicyd"'`.

## Building locally without releasing

- **Debian package:** on macOS, `./build-deb.sh` builds in the Docker build container (`VERSION=1.0.1 ARCHES="amd64 arm64" ./build-deb.sh`). On Linux, run `VERSION=1.0.1 ARCH=amd64 scripts/package-deb.sh` directly.
- **Every file, without a release:** run the Release workflow by hand from the Actions tab. It keeps the files as a workflow artifact.
