#!/usr/bin/env bash
# Download and unpack Hadoop, Pig, Hive and Spark into $HOME (the locations configure_environment.sh looks for).
# Skips anything already installed. Needs internet access and roughly 3 GB of free disk space.
# Versions can be overridden:  SPARK_VERSION=3.5.8 bash setup/install_toolchain.sh
set -euo pipefail

HADOOP_VERSION="${HADOOP_VERSION:-3.3.6}"
PIG_VERSION="${PIG_VERSION:-0.17.0}"
HIVE_VERSION="${HIVE_VERSION:-4.0.1}"
SPARK_VERSION="${SPARK_VERSION:-3.5.9}"
BASE="${APACHE_MIRROR:-https://archive.apache.org/dist}"
CACHE="$HOME/Downloads/fraudlens"
mkdir -p "$CACHE"

AVAIL_MB=$(df -Pm "$HOME" | awk 'NR==2 {print $4}')
if (( AVAIL_MB < 3500 )); then
    echo "WARNING: only ${AVAIL_MB} MB free in $HOME; the toolchain needs about 3500 MB (downloads + unpacked)."
fi

install_tool() {   # install_tool <name> <url> <unpacked dir name> <link name or ""> <executable relative to dir>
    local name="$1" url="$2" dir="$3" link="$4" exe="$5"
    local target="$HOME/$dir" archive="$CACHE/$(basename "$url")"
    if [[ -x "$target/$exe" || ( -n "$link" && -x "$HOME/$link/$exe" ) ]]; then
        echo "$name already installed; skipping."
        return 0
    fi
    echo "Downloading $name from $url"
    curl -fL --retry 3 --retry-delay 5 -C - -o "$archive" "$url" || {
        echo "ERROR: download failed for $name. If this version no longer exists, set ${name^^}_VERSION to another one."
        return 1
    }
    tar -tzf "$archive" >/dev/null 2>&1 || { echo "ERROR: $archive is corrupt; delete it and rerun."; return 1; }
    echo "Unpacking $name ..."
    tar -xzf "$archive" -C "$HOME"
    [[ -n "$link" && ! -e "$HOME/$link" ]] && ln -sfn "$target" "$HOME/$link"
    [[ -x "$target/$exe" ]] || { echo "ERROR: $name unpacked but $target/$exe is missing."; return 1; }
    echo "$name installed in $target"
}

install_tool hadoop "$BASE/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" "hadoop-$HADOOP_VERSION" hadoop bin/hdfs
install_tool pig    "$BASE/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz"                       "pig-$PIG_VERSION"    ""     bin/pig
install_tool hive   "$BASE/hive/hive-$HIVE_VERSION/apache-hive-$HIVE_VERSION-bin.tar.gz"       "apache-hive-$HIVE_VERSION-bin" hive bin/beeline
install_tool spark  "$BASE/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz"     "spark-$SPARK_VERSION-bin-hadoop3" "" bin/spark-submit

echo
echo "Toolchain ready. Next: bash setup/configure_environment.sh"
