#!/bin/bash
# Turns a release's macOS tarballs into signed, notarized and stapled disk
# images, on a Mac that holds the Developer ID certificate, and publishes the
# release. The same approach as pool-coordinator's script of this name.
#
# The release workflow builds and tests from the tag and leaves the release a
# draft holding ricochet_X.Y.Z_darwin_{arm64,amd64}.tar.gz. A binary built
# there is not Developer ID signed, so macOS kills a downloaded copy. For each
# architecture this:
#   - takes the tarball and checks it against the release's SHA256SUMS;
#   - signs the binary (hardened runtime, secure timestamp);
#   - packs the directory into a disk image, signs it, has Apple notarize it
#     and staples the ticket to it. A bare binary cannot hold a ticket, and a
#     Mac that cannot look one up online then refuses it; the image carries
#     its own.
#   - checks it as a downloader gets it: the image quarantined, the directory
#     copied out, the program run (amd64 under Rosetta). spctl only judges
#     app bundles, so running it is the check.
# Then it replaces the tarballs with the images in the release, rewrites
# SHA256SUMS, and publishes the release if it is a draft. It works on a
# published release too, replacing the files in place.
#
#   NOTARY_PROFILE=<keychain profile> scripts/sign-macos-release.sh v1.0.0
#
#   NOTARY_PROFILE  a profile made with `xcrun notarytool store-credentials`
#   SIGN_IDENTITY   defaults to the Werkswinkel Developer ID Application
set -euo pipefail
cd "$(dirname "$0")/.."

tag="${1:?usage: sign-macos-release.sh <tag>}"
version="${tag#v}"
profile="${NOTARY_PROFILE:?set NOTARY_PROFILE to a notarytool keychain profile}"
identity="${SIGN_IDENTITY:-Developer ID Application: Werkswinkel Pte Ltd (32XLPKQ5TF)}"
arches="arm64 amd64"

work=$(mktemp -d "${TMPDIR:-/tmp}/sign-macos.XXXXXX")
cleanup() {
    hdiutil detach -quiet "$work/check/mnt" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

step() { printf '\n== %s\n' "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

xcrun notarytool history --keychain-profile "$profile" >/dev/null 2>&1 ||
    die "no notarytool keychain profile named $profile"
arch -x86_64 /usr/bin/true 2>/dev/null ||
    die "Rosetta is not installed, so the amd64 program cannot be checked (softwareupdate --install-rosetta)"

step "the release's tarballs and checksums"
patterns=(--pattern SHA256SUMS)
for a in $arches; do patterns+=(--pattern "ricochet_${version}_darwin_${a}.tar.gz"); done
gh release download "$tag" "${patterns[@]}" --dir "$work"
sums_before=$(wc -l < "$work/SHA256SUMS" | tr -d ' ')

for a in $arches; do
    name="ricochet_${version}_darwin_${a}"
    tarball="${name}.tar.gz"
    image="${name}.dmg"
    run=(); [ "$a" = amd64 ] && run=(arch -x86_64)

    step "$a: check and unpack $tarball"
    [ -f "$work/$tarball" ] || die "$tag has no $tarball"
    (cd "$work" && grep " ${tarball}\$" SHA256SUMS | shasum -a 256 -c -)
    volume="$work/volume-$a"
    mkdir -p "$volume"
    tar -xzf "$work/$tarball" -C "$volume"
    dir="$volume/$name"
    [ "$(ls "$volume")" = "$name" ] || die "$tarball should hold only $name/"
    xattr -cr "$dir"

    step "$a: sign the program"
    codesign --force --timestamp --options runtime \
        --identifier com.stephanfeb.ricochet --sign "$identity" "$dir/ricochet"
    codesign --verify --strict --verbose=1 "$dir/ricochet"
    details=$(codesign -d --verbose=2 "$dir/ricochet" 2>&1)
    case "$details" in *"flags="*"(runtime)"*) ;; *) die "$a binary is not under the hardened runtime" ;; esac
    case "$details" in *"Timestamp="*) ;; *) die "$a binary has no secure timestamp" ;; esac
    ran=$(cd / && ${run[@]+"${run[@]}"} "$dir/ricochet" --version 2>&1) || die "the signed $a program does not run: $ran"
    [ "$ran" = "ricochet $version" ] || die "$a --version says $ran"

    step "$a: the disk image"
    hdiutil create -quiet -volname "ricochet $version $a" -srcfolder "$volume" -fs HFS+ -format UDZO -ov "$work/$image"
    codesign --force --timestamp --sign "$identity" "$work/$image"
    codesign --verify --strict "$work/$image"

    step "$a: notarize and staple"
    result=$(xcrun notarytool submit "$work/$image" --keychain-profile "$profile" --wait --output-format json) ||
        die "the submission failed: $result"
    id=$(printf '%s' "$result" | plutil -extract id raw -o - -)
    status=$(printf '%s' "$result" | plutil -extract status raw -o - -)
    echo "notarization $id: $status"
    if [ "$status" != Accepted ]; then
        xcrun notarytool log "$id" --keychain-profile "$profile" >&2 || true
        die "not accepted"
    fi
    xcrun stapler staple "$work/$image"
    xcrun stapler validate "$work/$image"

    step "$a: as a downloader gets it"
    # the image quarantined as a browser marks it, the directory copied out
    # carrying the mark, the program run from elsewhere: macOS kills a
    # quarantined program it cannot vouch for
    rm -rf "$work/check"
    mkdir -p "$work/check/mnt"
    cp "$work/$image" "$work/check/"
    mark="0081;$(printf '%x' "$(date +%s)");Safari;"
    xattr -w com.apple.quarantine "$mark" "$work/check/$image"
    hdiutil attach -quiet -nobrowse -readonly -mountpoint "$work/check/mnt" "$work/check/$image"
    listed=$(cd "$work/check/mnt" && find . -mindepth 1 -not -path './.fseventsd*' | sort)
    expected=$(cd "$volume" && find . -mindepth 1 | sort)
    [ "$listed" = "$expected" ] || die "the $a image does not hold exactly the tarball's files"
    ditto "$work/check/mnt/$name" "$work/check/$name"
    hdiutil detach -quiet "$work/check/mnt"
    find "$work/check/$name" -type f -exec xattr -w com.apple.quarantine "$mark" {} \;
    ran=$(cd / && ${run[@]+"${run[@]}"} "$work/check/$name/ricochet" --version 2>&1) ||
        die "macOS does not run the quarantined $a program: ${ran:-killed}"
    [ "$ran" = "ricochet $version" ] || die "quarantined $a --version says $ran"
    echo "$ran"

    # the image's line in place of the tarball's
    (cd "$work" && { grep -v " ${tarball}\$" SHA256SUMS; shasum -a 256 "$image"; } | sort -k2 > SHA256SUMS.new && mv SHA256SUMS.new SHA256SUMS)
    (cd "$work" && grep " ${image}\$" SHA256SUMS | shasum -a 256 -c -)
done

step "into $tag: the images in place of the tarballs, and SHA256SUMS"
[ "$(wc -l < "$work/SHA256SUMS" | tr -d ' ')" -eq "$sums_before" ] ||
    die "SHA256SUMS should still list $sums_before files: $(cat "$work/SHA256SUMS")"
cat "$work/SHA256SUMS"
images=(); for a in $arches; do images+=("$work/ricochet_${version}_darwin_${a}.dmg"); done
gh release upload "$tag" "${images[@]}" "$work/SHA256SUMS" --clobber
for a in $arches; do gh release delete-asset "$tag" "ricochet_${version}_darwin_${a}.tar.gz" --yes; done

if [ "$(gh release view "$tag" --json isDraft --jq .isDraft)" = true ]; then
    gh release edit "$tag" --draft=false
    echo "published $tag"
fi
gh release view "$tag" --json url,assets --jq '.url, (.assets[] | "  \(.name) \(.size)")'
