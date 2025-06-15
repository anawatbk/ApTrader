# Shell Scripts Module

This module contains shell scripts for deploying and managing AWS resources for the ApTrader project.

## Scripts

### `deploy_glue_etl.sh`

Comprehensive script for deploying and executing the ETL job on AWS Glue Python Shell.

#### Prerequisites

1. **AWS CLI** installed and configured
2. **IAM Role** with name `AWSGlueServiceRole` (or set `GLUE_ROLE_NAME` env var)
3. **S3 bucket** for storing Glue scripts
4. **Permissions** for Glue and S3 operations

#### Usage

```bash
# Deploy ETL script to AWS Glue
./shell_scripts/deploy_glue_etl.sh deploy my-glue-scripts-bucket

# Run ETL job for a specific year
./shell_scripts/deploy_glue_etl.sh run 2024

# Monitor a running job
./shell_scripts/deploy_glue_etl.sh monitor jr_1234567890abcdef

# List existing Glue jobs
./shell_scripts/deploy_glue_etl.sh list

# Delete the Glue job
./shell_scripts/deploy_glue_etl.sh delete

# Show help
./shell_scripts/deploy_glue_etl.sh help
```

#### Environment Variables

- `GLUE_ROLE_NAME`: IAM role name (default: `AWSGlueServiceRole`)
- `JOB_NAME`: Glue job name (default: `csv-to-parquet-etl`)

#### Features

- ✅ **Automated deployment** - Uploads script and creates Glue job
- ✅ **Job execution** - Runs ETL with year parameter
- ✅ **Progress monitoring** - Real-time job status updates
- ✅ **Error handling** - Comprehensive error checking and reporting
- ✅ **Interactive prompts** - Confirms destructive operations
- ✅ **Colored output** - Easy-to-read status messages

#### IAM Role Requirements

The script requires an IAM role with the following policies:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::anawatp-us-stocks/*",
                "arn:aws:s3:::anawatp-us-stocks",
                "arn:aws:s3:::your-glue-scripts-bucket/*",
                "arn:aws:s3:::your-glue-scripts-bucket"
            ]
        }
    ]
}
```

Plus the AWS managed policy: `service-role/AWSGlueServiceRole`

#### Examples

**Complete workflow:**

```bash
# 1. Deploy the ETL script
./shell_scripts/deploy_glue_etl.sh deploy my-glue-scripts-bucket

# 2. Run ETL for 2024 data
./shell_scripts/deploy_glue_etl.sh run 2024

# 3. Check job status
./shell_scripts/deploy_glue_etl.sh list
```

**Update existing job:**

```bash
# Script will prompt to update if job exists
./shell_scripts/deploy_glue_etl.sh deploy my-glue-scripts-bucket
```

**Cleanup:**

```bash
# Remove the Glue job
./shell_scripts/deploy_glue_etl.sh delete
```