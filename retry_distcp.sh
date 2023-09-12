#!/bin/bash


# ./retry_distcp.sh /path/to/copied_failed_paths_${timestamp}.txt
# ./retry_distcp.sh  "${dir_path}/copied_failed_paths_${timestamp}.txt"

# Set the hardcoded dir_path and log_dir
dir_path="/tmp/test"
log_dir="/tmp/test/logs"

# Retry failed DistCp jobs

if [ $# -ne 1 ]; then
    echo "Usage: $0 <failed_paths_file>"
    exit 1
fi

failed_paths_file="$1"

if [ ! -f "$failed_paths_file" ]; then
    echo "Error: Failed paths file '$failed_paths_file' not found."
    exit 1
fi

# Read failed paths from the file
failed_paths=($(cat "$failed_paths_file"))

# Set the same variables as in the original script
s3_bucket="pulse-s3-upload"
s3_endpoint="s3.ap-south-1.amazonaws.com"
keytab_path="/etc/security/keytabs/hdfs.headless.keytab"
credential_provider="jceks://hdfs/tmp/hdfs.jceks"
hdfs_default_fs=$(grep -A1 '<name>fs.defaultFS</name>' /etc/hadoop/conf/core-site.xml | grep '<value>' | sed -e 's/<value>//;s/<\/value>//')

# Set retry log filenames
retry_success_log="$failed_paths_file.retry_success.log"
retry_failure_log="$failed_paths_file.retry_failure.log"



# Create a new file for the current retry
retry_failed_paths_file="retry_failed_paths_$(date +"%Y%m%d%H%M%S").txt"
retry_success_paths_file="retry_success_paths_$(date +"%Y%m%d%H%M%S").txt"

# Function to retry a single failed DistCp job
retry_distcp() {
    local path="$1"
    log_file="${log_dir}/distcp_retry_${timestamp}_${path//\//-}.log"
    console_log_file="${log_dir}/console_retry_${timestamp}_${path//\//-}.log"

    hadoop distcp \
      -Dipc.client.fallback-to-simple-auth-allowed=true \
      -Dhadoop.security.credential.provider.path=$credential_provider \
      -Dfs.s3a.bucket.$s3_bucket.endpoint=$s3_endpoint \
      -skipcrccheck -overwrite -pc \
      -strategy dynamic \
      "$hdfs_default_fs$path" s3a://$s3_bucket$path &> "$console_log_file"
    
    exit_status=$?
    if [ $exit_status -ne 0 ]; then
        echo "$path" >> "$retry_failed_paths_file"
        echo "DistCp retry failed for: $path" >> "$retry_failure_log"
    else
        echo "$path" >> "$retry_success_paths_file"
        echo "DistCp retry successful for: $path" >> "$retry_success_log"
    fi
}

# Retry DistCp jobs for all failed paths
for path in "${failed_paths[@]}"; do
    retry_distcp "$path"
done

# Rename the input file to mark it as completed
mv "$failed_paths_file" "${failed_paths_file}.completed"

echo "DistCp retry process completed."
