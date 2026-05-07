#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

./scripts/build.sh

find src/test/java -name '*.java' | sort > build/test-sources.txt
javac --release 17 -cp build/classes -d build/classes @build/test-sources.txt

java -cp build/classes com.example.minikafka.TestRunner
