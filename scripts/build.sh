#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

rm -rf build
mkdir -p build/classes

find src/main/java -name '*.java' | sort > build/main-sources.txt
javac --release 17 -d build/classes @build/main-sources.txt

jar --create --file build/mini-kafka.jar --main-class com.example.minikafka.Main -C build/classes .

echo "Built build/mini-kafka.jar"
