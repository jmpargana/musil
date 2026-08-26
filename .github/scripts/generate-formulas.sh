#!/usr/bin/env bash
set -euo pipefail

REPO="jmpargana/musil"

declare -A FORMULA_MAP
FORMULA_MAP[server]="musil-broker"
FORMULA_MAP[producer]="musil-producer"
FORMULA_MAP[consumer]="musil-consumer"
FORMULA_MAP[seeder]="musil-seeder"
FORMULA_MAP[musil]="musil"

declare -A DESC_MAP
DESC_MAP[server]="Kafka-compatible message broker"
DESC_MAP[producer]="CLI producer client for musil"
DESC_MAP[consumer]="CLI consumer client for musil"
DESC_MAP[seeder]="Topic/partition seeder for musil"
DESC_MAP[musil]="Kafka-compatible message broker and CLI toolkit"

TARGETS=(
  "x86_64-apple-darwin"
  "aarch64-apple-darwin"
  "x86_64-unknown-linux-gnu"
  "aarch64-unknown-linux-gnu"
)

mkdir -p homebrew-tools/Formula

for bin in server producer consumer seeder musil; do
  FORMULA_NAME="${FORMULA_MAP[$bin]}"
  CLASS_NAME=$(echo "$FORMULA_NAME" | sed 's/-/_/g' | sed 's/\(^\|_\)\([a-z]\)/\U\2/g')
  DESC="${DESC_MAP[$bin]}"

  declare -A SHAS
  for target in "${TARGETS[@]}"; do
    ARCHIVE="musil-${bin}-${VERSION}-${target}.tar.gz"
    gh release download "$TAG" --repo "$REPO" --pattern "$ARCHIVE" --dir /tmp/dl
    SHAS[$target]=$(sha256sum "/tmp/dl/${ARCHIVE}" | cut -d' ' -f1)
    rm -f "/tmp/dl/${ARCHIVE}"
  done

  cat > "homebrew-tools/Formula/${FORMULA_NAME}.rb" <<RUBY
class ${CLASS_NAME} < Formula
  desc "${DESC}"
  homepage "https://github.com/${REPO}"
  version "${VERSION}"
  license "MIT"

  on_macos do
    if Hardware::CPU.intel?
      url "https://github.com/${REPO}/releases/download/${TAG}/musil-${bin}-${VERSION}-x86_64-apple-darwin.tar.gz"
      sha256 "${SHAS[x86_64-apple-darwin]}"
    end
    if Hardware::CPU.arm?
      url "https://github.com/${REPO}/releases/download/${TAG}/musil-${bin}-${VERSION}-aarch64-apple-darwin.tar.gz"
      sha256 "${SHAS[aarch64-apple-darwin]}"
    end
  end

  on_linux do
    if Hardware::CPU.intel?
      url "https://github.com/${REPO}/releases/download/${TAG}/musil-${bin}-${VERSION}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "${SHAS[x86_64-unknown-linux-gnu]}"
    end
    if Hardware::CPU.arm?
      url "https://github.com/${REPO}/releases/download/${TAG}/musil-${bin}-${VERSION}-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "${SHAS[aarch64-unknown-linux-gnu]}"
    end
  end

  def install
    bin.install "${bin}" => "${FORMULA_NAME}"
  end

  test do
    assert_match "${FORMULA_NAME}", shell_output("#{bin}/${FORMULA_NAME} --help", 2)
  end
end
RUBY

  unset SHAS
  declare -A SHAS
done
