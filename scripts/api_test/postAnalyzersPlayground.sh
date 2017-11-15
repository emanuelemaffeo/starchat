#!/usr/bin/env bash

ANALYZER=${1:-"keyword(\\\"test\\\")"}
QUERY=${2:-"this is a test"}
DATA=${3:-"{\"item_list\": [], \"extracted_variables\":{}}"}
PORT=${4:-8888}
INDEX_NAME=${5:-index_0}
curl -H "Content-Type: application/json" -X POST "http://localhost:${PORT}/${INDEX_NAME}/analyzers_playground" -d "
{
	\"analyzer\": \"${ANALYZER}\",
	\"query\": \"${QUERY}\",
	\"data\": ${DATA}
}
"

