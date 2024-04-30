#!/bin/bash
cd ..

PROJECT_ROOT=$(dirname "$(dirname "$0")")
# shellcheck disable=SC2164
cd "$PROJECT_ROOT"

python3 -m enbios.dev.create_experiment_schema
python3 -m enbios.dev.docs_api_inject