#!/bin/bash
#
# Install or update neo4j-exporter on Linux systems.
# Supports Alpine (apk), Debian/Ubuntu (deb), and RHEL/CentOS/Fedora (rpm).
#

set -euo pipefail

REPO="PapaDanielVi/neo4j-exporter"
BINARY_NAME="neo4j-exporter"
INSTALL_DIR="${INSTALL_DIR:-/usr/local/bin}"

# Detect architecture
detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64) echo "x86_64" ;;
        aarch64|arm64) echo "arm64" ;;
        armv7l) echo "arm" ;;
        *) echo "x86_64" ;;
    esac
}

ARCH=$(detect_arch)

# Detect package manager and get download URL
get_download_url() {
    if command -v apk &>/dev/null; then
        echo "https://github.com/${REPO}/releases/download/latest/neo4j-exporter_linux_${ARCH}.apk"
        PACKAGE_FORMAT="apk"
    elif command -v apt-get &>/dev/null; then
        echo "https://github.com/${REPO}/releases/download/latest/neo4j-exporter_linux_${ARCH}.deb"
        PACKAGE_FORMAT="deb"
    elif command -v dnf &>/dev/null || command -v yum &>/dev/null; then
        echo "https://github.com/${REPO}/releases/download/latest/neo4j-exporter_linux_${ARCH}.rpm"
        PACKAGE_FORMAT="rpm"
    else
        # Fallback to tar.gz archive
        echo "https://github.com/${REPO}/releases/download/latest/neo4j-exporter_Linux_${ARCH}.tar.gz"
        PACKAGE_FORMAT="tar"
    fi
}

# Get latest release version
get_latest_version() {
    curl -s "https://api.github.com/repos/${REPO}/releases/latest" | grep '"tag_name"' | sed -E 's/.*"([^"]+)".*/\1/'
}

# Check if neo4j-exporter is installed
get_installed_version() {
    if [[ -x "${INSTALL_DIR}/${BINARY_NAME}" ]]; then
        echo "installed"
    else
        echo "not installed"
    fi
}

# Download and install
download_and_install() {
    local url="$1"
    local tmp_dir
    tmp_dir=$(mktemp -d)
    local tmp_file

    echo "Downloading neo4j-exporter ${VERSION} for ${ARCH}..."
    tmp_file="${tmp_dir}/neo4j-exporter.${PACKAGE_FORMAT}"

    curl -sL -o "${tmp_file}" "${url}"

    if [[ "${PACKAGE_FORMAT}" == "tar" ]]; then
        # Extract tar.gz archive
        tar -xzf "${tmp_file}" -C "${tmp_dir}"
        sudo install -m 755 "${tmp_dir}/${BINARY_NAME}" "${INSTALL_DIR}/${BINARY_NAME}"
    elif [[ "${PACKAGE_FORMAT}" == "apk" ]]; then
        # Alpine apk package
        sudo apk add --allow-untrusted "${tmp_file}"
    elif [[ "${PACKAGE_FORMAT}" == "deb" ]]; then
        # Debian/Ubuntu deb package
        sudo dpkg -i "${tmp_file}" || sudo apt-get install -f -y
    elif [[ "${PACKAGE_FORMAT}" == "rpm" ]]; then
        # RHEL/CentOS/Fedora rpm package
        sudo rpm -U "${tmp_file}"
    fi

    rm -rf "${tmp_dir}"
}

# Main installation logic
main() {
    VERSION=$(get_latest_version)
    CURRENT_VERSION=$(get_installed_version)

    echo "Latest version: ${VERSION}"
    echo "Installed: ${CURRENT_VERSION}"

    URL=$(get_download_url)
    echo "Download URL: ${URL}"

    # Verify URL is accessible
    if ! curl -sfI "${URL}" &>/dev/null; then
        echo "Warning: Package format not found, falling back to tar.gz archive..."
        URL="https://github.com/${REPO}/releases/download/${VERSION}/neo4j-exporter_Linux_${ARCH}.tar.gz"
        PACKAGE_FORMAT="tar"
    fi

    download_and_install "${URL}"

    # Verify installation
    if [[ -x "${INSTALL_DIR}/${BINARY_NAME}" ]]; then
        echo "Installation successful!"
        echo "Installed: ${INSTALL_DIR}/${BINARY_NAME}"
    else
        echo "Installation failed. Binary not found at ${INSTALL_DIR}/${BINARY_NAME}"
        exit 1
    fi
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --install-dir)
            INSTALL_DIR="$2"
            shift 2
            ;;
        --help|-h)
            echo "Usage: $0 [--install-dir DIR]"
            echo "  --install-dir DIR  Installation directory (default: /usr/local/bin)"
            echo ""
            echo "Environment variables:"
            echo "  INSTALL_DIR      Same as --install-dir (default: /usr/local/bin)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

main