#!/usr/bin/env bash
set -e

# Set the install directory for Go binaries
export GOBIN="${HOME}/go/bin"

echo "Installing protoc plugins to $GOBIN ..."

go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

echo "Done. Make sure $GOBIN is in your PATH."

echo "Checking for required tools..."
# These tools are required by the Makefile in this directory
REQUIRED_TOOLS=(protoc protoc-gen-go protoc-gen-go-grpc)
MISSING=0
for tool in "${REQUIRED_TOOLS[@]}"; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Error: $tool is not installed or not in your PATH." >&2
        MISSING=1
    else
        echo "$tool found: $(command -v $tool)"
    fi
done

if [ $MISSING -eq 1 ]; then
    echo "Some required tools are missing. Please ensure all are installed and in your PATH."
    exit 1
else
    echo "All required tools for the Makefile are available."
fi
