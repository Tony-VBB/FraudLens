#!/usr/bin/env bash
# FraudLens: install OS-level prerequisites (Ubuntu only).
# Hadoop, Pig, Hive and Spark are NOT installed here; see "Installing the toolchain" in README.md.
set -euo pipefail

echo "========== FraudLens: System Prerequisites =========="

if [[ ! -f /etc/os-release ]]; then
    echo "ERROR: Cannot identify the operating system."
    exit 1
fi

. /etc/os-release

if [[ "${ID:-}" != "ubuntu" ]]; then
    echo "ERROR: This installer currently supports Ubuntu only."
    exit 1
fi

echo "Operating system: $PRETTY_NAME"

if ! command -v apt-get >/dev/null 2>&1; then
    echo "ERROR: apt-get is unavailable."
    exit 1
fi

echo
echo "Installing system dependencies (Java 8, Python build/runtime helpers, SSH for Hadoop)..."

sudo apt-get update
sudo apt-get install -y \
    bash curl wget tar gzip unzip zip procps \
    openssh-client openssh-server rsync \
    openjdk-8-jdk \
    python3 python3-pip python3-venv python3-numpy python3-packaging python3-setuptools

echo
echo "Checking Java 8..."
JAVA8_FOUND=false
for candidate in \
    /usr/lib/jvm/java-8-openjdk-amd64 \
    /usr/lib/jvm/temurin-8-jdk-amd64 \
    /usr/lib/jvm/temurin-8-jdk
do
    if [[ -x "$candidate/bin/java" ]] &&
       "$candidate/bin/java" -version 2>&1 | grep -q 'version "1\.8\.'; then
        echo "Java 8 found: $candidate"
        JAVA8_FOUND=true
        break
    fi
done
if [[ "$JAVA8_FOUND" != true ]]; then
    echo "WARNING: Java 8 was not found in the usual installation paths."
    echo "Install a Java 8 JDK manually and set JAVA_HOME before configure_environment.sh."
fi

echo
echo "Checking Python 3.12 (required by the Spark scripts)..."
if command -v python3.12 >/dev/null 2>&1; then
    python3.12 -c 'import numpy, packaging, setuptools; print("python3.12 OK: numpy", numpy.__version__)'
else
    echo "WARNING: python3.12 not found. Use Ubuntu 24.04, or set PYSPARK_PYTHON in config/fraudlens.env"
    echo "to another interpreter that has numpy, packaging and setuptools installed."
fi

echo
echo "System prerequisites installed."
echo "Next: install Hadoop/Pig/Hive/Spark (README.md), then run setup/configure_environment.sh"
