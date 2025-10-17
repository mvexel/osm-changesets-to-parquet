#!/bin/bash
# Helper script for managing Cloudflare R2 bucket
# This can be run locally for testing or maintenance

set -e

# Configuration - Update these with your values
R2_BUCKET_NAME="${R2_BUCKET_NAME:-osm-changesets}"
R2_ACCOUNT_ID="${R2_ACCOUNT_ID}"
R2_ACCESS_KEY_ID="${R2_ACCESS_KEY_ID}"
R2_SECRET_ACCESS_KEY="${R2_SECRET_ACCESS_KEY}"

# Check if credentials are set
if [ -z "$R2_ACCOUNT_ID" ] || [ -z "$R2_ACCESS_KEY_ID" ] || [ -z "$R2_SECRET_ACCESS_KEY" ]; then
    echo "Error: R2 credentials not set!"
    echo ""
    echo "Please set these environment variables:"
    echo "  export R2_ACCOUNT_ID='your-account-id'"
    echo "  export R2_ACCESS_KEY_ID='your-access-key-id'"
    echo "  export R2_SECRET_ACCESS_KEY='your-secret-access-key'"
    echo ""
    echo "Or source them from a file:"
    echo "  source ~/.r2-credentials"
    exit 1
fi

ENDPOINT_URL="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"

# AWS CLI configuration
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
export AWS_DEFAULT_REGION="auto"
export AWS_ENDPOINT_URL="$ENDPOINT_URL"

# Command functions
cmd_list() {
    echo "=== Listing files in bucket: $R2_BUCKET_NAME ==="
    aws s3 ls "s3://${R2_BUCKET_NAME}/" --recursive --human-readable --summarize
}

cmd_upload() {
    local file="$1"
    if [ -z "$file" ]; then
        echo "Error: No file specified"
        echo "Usage: $0 upload <file.parquet>"
        exit 1
    fi

    if [ ! -f "$file" ]; then
        echo "Error: File not found: $file"
        exit 1
    fi

    local basename=$(basename "$file")
    echo "=== Uploading $file to s3://${R2_BUCKET_NAME}/${basename} ==="
    aws s3 cp "$file" "s3://${R2_BUCKET_NAME}/${basename}" \
        --content-type "application/octet-stream" \
        # --progress

    echo "✅ Upload complete!"
    echo "File should be accessible at your R2 public URL"
}

cmd_delete() {
    local filename="$1"
    if [ -z "$filename" ]; then
        echo "Error: No filename specified"
        echo "Usage: $0 delete <filename>"
        exit 1
    fi

    echo "=== Deleting s3://${R2_BUCKET_NAME}/${filename} ==="
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        aws s3 rm "s3://${R2_BUCKET_NAME}/${filename}"
        echo "✅ Deleted!"
    else
        echo "Cancelled."
    fi
}

cmd_cleanup() {
    echo "=== Cleaning up old files ==="
    echo "This will keep the latest file and the 5 most recent timestamped files"
    read -p "Continue? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Cancelled."
        exit 0
    fi

    # Get all parquet files except changesets.parquet
    aws s3 ls "s3://${R2_BUCKET_NAME}/" --recursive | \
        grep -E "changesets-[0-9].*\.parquet$" | \
        awk '{print $4}' | \
        sort -r | \
        tail -n +6 | \
        while read file; do
            echo "Deleting old file: $file"
            aws s3 rm "s3://${R2_BUCKET_NAME}/${file}"
        done

    echo "✅ Cleanup complete!"
}

cmd_info() {
    local filename="${1:-changesets.parquet}"
    echo "=== File info: $filename ==="
    aws s3api head-object \
        --bucket "$R2_BUCKET_NAME" \
        --key "$filename" \
        --endpoint-url "$ENDPOINT_URL" || echo "File not found"
}

cmd_public_url() {
    echo "=== Your R2 Public URLs ==="
    echo ""
    echo "R2.dev domain (free):"
    echo "  https://pub-${R2_ACCOUNT_ID}.r2.dev/${R2_BUCKET_NAME}/changesets.parquet"
    echo ""
    echo "To set up a custom domain:"
    echo "  1. Go to Cloudflare R2 dashboard"
    echo "  2. Click on your bucket"
    echo "  3. Settings > Custom Domains > Connect Domain"
    echo ""
    echo "Then update the R2_PUBLIC_URL in your workflow."
}

cmd_test_duckdb() {
    local url="$1"
    if [ -z "$url" ]; then
        echo "Error: No URL specified"
        echo "Usage: $0 test-duckdb <public-url>"
        echo "Example: $0 test-duckdb https://pub-xxx.r2.dev/osm-changesets/changesets.parquet"
        exit 1
    fi

    echo "=== Testing DuckDB remote query ==="
    echo "URL: $url"
    echo ""

    if ! command -v duckdb &> /dev/null; then
        echo "Error: duckdb not found. Please install it:"
        echo "  brew install duckdb  (macOS)"
        echo "  wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip"
        exit 1
    fi

    echo "Running: SELECT COUNT(*) FROM '$url'"
    duckdb -c "SELECT COUNT(*) as total_changesets FROM '$url'"
}

# Help command
cmd_help() {
    cat <<EOF
Cloudflare R2 Management Script

Usage: $0 <command> [arguments]

Commands:
  list                    List all files in the bucket
  upload <file>           Upload a parquet file to R2
  delete <filename>       Delete a file from R2
  cleanup                 Remove old files (keeps 5 most recent)
  info [filename]         Show metadata for a file (default: changesets.parquet)
  public-url              Show your public R2 URLs
  test-duckdb <url>       Test DuckDB remote query against a URL
  help                    Show this help message

Environment Variables:
  R2_ACCOUNT_ID           Your Cloudflare account ID (required)
  R2_ACCESS_KEY_ID        Your R2 access key ID (required)
  R2_SECRET_ACCESS_KEY    Your R2 secret access key (required)
  R2_BUCKET_NAME          Bucket name (default: osm-changesets)

Examples:
  # List files
  $0 list

  # Upload a file
  $0 upload changesets-latest.parquet

  # Test remote query
  $0 test-duckdb https://pub-xxx.r2.dev/osm-changesets/changesetd.parquet

  # Get public URLs
  $0 public-url
EOF
}

# Main command router
COMMAND="${1:-help}"
shift || true

case "$COMMAND" in
    list)
        cmd_list
        ;;
    upload)
        cmd_upload "$@"
        ;;
    delete)
        cmd_delete "$@"
        ;;
    cleanup)
        cmd_cleanup
        ;;
    info)
        cmd_info "$@"
        ;;
    public-url)
        cmd_public_url
        ;;
    test-duckdb)
        cmd_test_duckdb "$@"
        ;;
    help|--help|-h)
        cmd_help
        ;;
    *)
        echo "Error: Unknown command: $COMMAND"
        echo ""
        cmd_help
        exit 1
        ;;
esac
