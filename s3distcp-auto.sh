#!/bin/bash
# set -x
# Directory containing reportSnapshot files
dir_path="/tmp/test"
log_dir="/tmp/test/logs"
if [ ! -d "$log_dir" ]; then
    mkdir -p "$log_dir"
    echo "Log Directory created $log_dir."
fi

s3_bucket="pulse-s3-upload"
s3_endpoint="s3.ap-south-1.amazonaws.com"
keytab_path="/etc/security/keytabs/hdfs.headless.keytab"
credential_provider="jceks://hdfs/tmp/hdfs.jceks"
hdfs_default_fs=$(awk -F'[<>]' '/<name>fs.defaultFS<\/name>/{getline; print $3}' /etc/hadoop/conf/core-site.xml)

# Initialize the number of parallel tasks
max_parallel_tasks=5  # You can adjust this based on your system resources

# Array to store background job IDs
declare -a bg_job_ids

# Find the latest epoch time file
latest_file=$(ls -1t "$dir_path"/reportSnapshot_* 2>/dev/null | head -n 1)

# Check if any files were found
if [ -z "$latest_file" ]; then
    echo "No reportSnapshot_ files found matching the pattern in $dir_path."
    exit 1
fi

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed. Please install and configure the AWS CLI."
    exit 1
fi

# Check if the AWS CLI is configured with the proper credentials
aws sts get-caller-identity &> /dev/null
if [ $? -ne 0 ]; then
    echo "Error: AWS CLI is not properly configured. Please configure it with the appropriate IAM credentials."
    exit 1
fi

# Function to handle errors and exit
handle_error() {
    local error_code="$1"
    local error_message="$2"
    echo "Error: $error_message"
    exit "$error_code"
}

# Function to create placeholder in S3
create_placeholder_in_s3() {
    local s3_path="$1"
    aws s3api put-object --bucket $s3_bucket --key "$s3_path" --content-length 0 >/dev/null 2>&1
}

# Log file for the entire process
today_date=$(date +'%Y-%m-%d')
main_log_file="$dir_path/distcp_process_$today_date.log"
echo "Process started at $(date)" >> "$main_log_file"

# Log the latest epoch time file
echo "Latest epoch time file: $latest_file" >> "$main_log_file"


# Extract paths with specific markers from the latest file
marker_lines=$(grep -E "#MODIFIED|#CREATED" "$latest_file")

# Create a new file with all paths (before any DistCp operation)
timestamp=$(date +"%Y%m%d%H%M%S")
all_paths_file="${dir_path}/all_paths_${timestamp}.txt"
echo "$marker_lines" | cut -d '#' -f 1 > "$all_paths_file"

# Log file for DistCp success and failure
distcp_success_log="$dir_path/distcp_success.log"
distcp_failure_log="$dir_path/distcp_failure.log"

# Generate a new timestamped copied paths file
copied_paths_file="${dir_path}/copied_paths_${timestamp}.txt"
copied_failed_paths_file="${dir_path}/copied_failed_paths_${timestamp}.txt"

# Loop through each path and run DistCp in batches of 5
batch_count=0

for line in $(cat "$all_paths_file"); do
    path="${line%%##*}"
    console_log_file="${log_dir}/console_${timestamp}_${path//\//-}.log"

    kinit -kt $keytab_path $(klist -kt $keytab_path |sed -n "4p"|cut -d ' ' -f7) || \
        handle_error 1 "Kerberos authentication failed"

    hadoop distcp \
      -Dipc.client.fallback-to-simple-auth-allowed=true \
      -Dhadoop.security.credential.provider.path=$credential_provider \
      -Dfs.s3a.bucket.$s3_bucket.endpoint=$s3_endpoint \
      -skipcrccheck -overwrite -pc \
      -strategy dynamic \
      "$hdfs_default_fs$path" s3a://$s3_bucket$path &> "$console_log_file" &

    # Store the background job ID
    bg_job_ids+=($!)

    # Check if the current batch is complete
    if (( ${#bg_job_ids[@]} >= max_parallel_tasks )); then
        # Wait for the current batch to complete
        for job_id in "${bg_job_ids[@]}"; do
            wait "$job_id"
            exit_status=$?
            if [ $exit_status -ne 0 ]; then
                echo "Background job $job_id failed with exit status $exit_status"
                echo "$path" >> "$copied_failed_paths_file"
            else
                ((success_count++))
            fi
        done

        # Clear the background job IDs for the next batch
        bg_job_ids=()

        # Increment the batch count
        ((batch_count++))

        # If the batch count is a multiple of max_parallel_tasks, check if all jobs were successful
        if ((batch_count % max_parallel_tasks == 0)); then
            if [ "$success_count" -eq $max_parallel_tasks ]; then
                # All jobs in the batch were successful, write the paths to the copied_paths_file
                for path in "${bg_paths[@]}"; do
                    echo "$path" >> "$copied_paths_file"
                done
            else
                # At least one job in the batch failed, write paths to copied_failed_paths_file
                for path in "${bg_paths[@]}"; do
                    echo "$path" >> "$copied_failed_paths_file"
                done
            fi
            # Reset the success count
            success_count=0
        fi
    fi

    # Store the path in the batch for later evaluation
    bg_paths+=("$path")
done

# Wait for any remaining background jobs to finish
for job_id in "${bg_job_ids[@]}"; do
    wait "$job_id"
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        echo "Background job $job_id failed with exit status $exit_status"
        echo "$path" >> "$copied_failed_paths_file"
    else
        ((success_count++))
    fi
done

# Check if all remaining jobs were successful
if [ "$success_count" -eq ${#bg_paths[@]} ]; then
    # All remaining jobs were successful, write the paths to the copied_paths_file
    for path in "${bg_paths[@]}"; do
        echo "$path" >> "$copied_paths_file"
    done
else
    # At least one remaining job failed, write paths to copied_failed_paths_file
    for path in "${bg_paths[@]}"; do
        echo "$path" >> "$copied_failed_paths_file"
    done
fi

# Log process completion
echo "Process completed at $(date)" >> "$main_log_file"

echo "Process completed. Check log files for details."
