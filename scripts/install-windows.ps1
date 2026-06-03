# Install or update neo4j-exporter on Windows systems.
# Supports amd64 and arm64 architectures.

param(
    [string]$InstallDir = "$env:PROGRAMFILES\neo4j-exporter",
    [string]$Version = ""
)

$ErrorActionPreference = "Stop"

$Repo = "PapaDanielVi/neo4j-exporter"
$BinaryName = "neo4j-exporter.exe"

# Detect architecture
function Get-Architecture {
    $arch = (Get-CimInstance Win32_Processor).AddressWidth
    if ($arch -eq 64) {
        if ((Get-CimInstance Win32_ComputerSystem).PCSystemType -eq 3) {
            # ARM64 on Windows typically shows as 32-bit emulation or needs different detection
            $procArch = (Get-CimInstance Win32_Processor).Architecture
            if ($procArch -eq 12) { return "arm64" }
        }
        return "x86_64"
    }
    return "x86_64"
}

$Arch = Get-Architecture

# Get latest release version
function Get-LatestVersion {
    try {
        $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest" -Headers @{"Accept" = "application/json"}
        return $response.tag_name
    } catch {
        Write-Error "Failed to fetch latest version: $_"
        exit 1
    }
}

# Check if neo4j-exporter is installed
function Get-InstalledVersion {
    $binaryPath = Join-Path $InstallDir $BinaryName
    if (Test-Path $binaryPath) {
        return "installed"
    }
    return "not installed"
}

# Download and install
function Install-Binary {
    param([string]$Url)

    $tmpDir = New-TemporaryFile | ForEach-Object { Remove-Item $_; New-Item -ItemType Directory -Path $_ }
    $tmpFile = Join-Path $tmpDir "neo4j-exporter.tar.gz"

    Write-Host "Downloading neo4j-exporter $script:Version for $Arch..."
    Invoke-WebRequest -Uri $Url -OutFile $tmpFile

    # Extract tar.gz archive
    # PowerShell 5.1+ ships with tar.exe on Windows 10+/Server 2019+
    if (Get-Command tar -ErrorAction SilentlyContinue) {
        tar -xzf $tmpFile -C $tmpDir
    } elseif (Get-Command 7z -ErrorAction SilentlyContinue) {
        # Try 7-zip as fallback
        7z x $tmpFile -o$tmpDir -y | Out-Null
    } else {
        # PowerShell 7+ has Compress-Archive but needs tar support
        try {
            # Download and use the bundled Expand-TarGz function
            $sourceStream = [System.IO.Compression.GzipStream]::new(
                [System.IO.File]::OpenRead($tmpFile),
                [System.IO.Compression.CompressionMode]::Decompress
            )
            $tarStream = [System.IO.MemoryStream]::new()
            $sourceStream.CopyTo($tarStream)
            $sourceStream.Dispose()
            $tarStream.Position = 0

            # Note: Full tar extraction requires additional logic or external tools
            # PowerShell 7+ recommended: https://github.com/PowerShell/PowerShell/releases
            throw "No tar extraction tool found. Install PowerShell 7+, 7-Zip, or use Windows 10+ tar.exe."
        } catch {
            Write-Error "No tar extraction tool found. Please install PowerShell 7+, 7-Zip, or use Windows 10+ tar.exe."
            exit 1
        }
    }

    # Create install directory if it doesn't exist
    if (-not (Test-Path $InstallDir)) {
        New-Item -ItemType Directory -Path $InstallDir -Force | Out-Null
    }

    # Copy binary
    $sourceBinary = Join-Path $tmpDir $BinaryName
    Copy-Item -Path $sourceBinary -Destination (Join-Path $InstallDir $BinaryName) -Force

    # Cleanup
    Remove-Item -Path $tmpDir -Recurse -Force
}

# Main installation logic
function Main {
    if (-not $Version) {
        $script:Version = Get-LatestVersion
    }
    $currentVersion = Get-InstalledVersion

    Write-Host "Latest version: $Version"
    Write-Host "Currently installed: $currentVersion"

    # Determine download URL based on release assets
    $baseUrl = "https://github.com/$Repo/releases/download/$Version"
    $url = "$baseUrl/neo4j-exporter_Windows_$Arch.tar.gz"

    # Verify URL is accessible
    try {
        Invoke-WebRequest -Uri $url -Method Head -ErrorAction Stop | Out-Null
    } catch {
        Write-Warning "Package not found at $url, trying alternative formats..."
        # Try archive naming convention
        $url = "https://github.com/$Repo/releases/download/latest/neo4j-exporter_Windows_$Arch.tar.gz"
    }

    Install-Binary -Url $url

    $binaryPath = Join-Path $InstallDir $BinaryName
    if (Test-Path $binaryPath) {
        Write-Host "Installation successful!"
        Write-Host "Installed: $binaryPath"
    } else {
        Write-Error "Installation failed. Binary not found at $binaryPath"
        exit 1
    }
}

# Parse arguments
if ($Help -or $args -contains "-?" -or $args -contains "--help" -or $args -contains "/?") {
    Write-Host "Usage: .\install-windows.ps1 [-InstallDir DIR] [-Version TAG]"
    Write-Host "  -InstallDir DIR  Installation directory (default: C:\Program Files\neo4j-exporter)"
    Write-Host "  -Version TAG     Specific version to install (default: latest)"
    exit 0
}

Main