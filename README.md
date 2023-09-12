
The script named "s3distcp-auto" automates the process of copying modified or created files from a local directory to an Amazon S3 bucket using the AWS DataSync service. The script identifies recently modified or created files in a specified local directory, creates placeholders for them in S3, and then initiates the data copying process using the hadoop distcp utility. The script also handles authentication through Kerberos, logs the entire process, and provides a summary of successful and failed copying operations.

Usage:

The "s3distcp-auto" script simplifies the task of transferring data from a HDFS directory to an S3 bucket. It automates the process of identifying, creating placeholders, and copying data while maintaining comprehensive logs for tracking. This script is particularly useful for scenarios where data synchronization between a HDFS filesystem and an S3 bucket is needed.


**How to Use:**

1. Pre-requisites:

* Ensure that the AWS CLI is installed and configured with the proper IAM credentials.
* Configure Kerberos authentication for Hadoop (if applicable).
* Ensure the required keytab file and credential provider file are available.
* Verify that the necessary Hadoop and AWS configurations are in place.
* Adjust the S3 bucket name and endpoint in the script to match your setup.

https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.5/bk_cloud-data-access/content/s3-credential-providers.html

​*Creating a Credential File*

You can create a credential file on any Hadoop filesystem. When you create one on HDFS or a UNIX filesystem, the permissions are automatically set to keep the file private to the reader — though as directory permissions are not touched, you should verify that the directory containing the file is readable only by the current user. For example:
```
# Replace the -value with your Access and Secret key

hadoop credential create fs.s3a.access.key -value <access_key> \
    -provider jceks://hdfs/tmp/hdfs.jceks

hadoop credential create fs.s3a.secret.key -value <secret_key> \
    -provider jceks://hdfs/tmp/hdfs.jceks
```
After creating the credential file, you can list it to see what entries are kept inside it. For example:

```
hadoop credential list -provider jceks://hdfs/tmp/hdfs.jceks

Listing aliases for CredentialProvider: jceks://hdfs/tmp/hdfs.jceks
fs.s3a.secret.key
fs.s3a.access.key
```
2. Script Configuration:


* Set the `dir_path` variable to the directory containing the reportSnapshot files.
* Update the s3_placeholder_path with the appropriate S3 bucket name.

3. Execution:
Run the script using the following command:
`./s3distcp-auto.sh`

4. Output:

The script will create log files to track the entire process, including the main process log, DistCp success log, and DistCp failure log.
Successfully copied paths and paths that failed to copy will be listed in separate log files.
