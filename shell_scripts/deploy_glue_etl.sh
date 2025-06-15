#!/bin/bash

# AWS Glue ETL Deployment and Execution Script
# Usage: ./shell_scripts/deploy_glue_etl.sh [COMMAND] [OPTIONS]

set -e  # Exit on any error

# Configuration
AWS_REGION="us-west-2"
DEFAULT_S3_BUCKET="glue-scripts-bucket"
SCRIPT_NAME="s3_csv_to_s3_parquet_glue_shell_job.py"
LOCAL_SCRIPT_PATH="etl/${SCRIPT_NAME}"
JOB_NAME="csv-to-parquet-etl"
GLUE_ROLE_NAME="AWSGlueServiceRole"
PYARROW_VERSION="14.0.1"
MAX_CAPACITY=1
TIMEOUT=2880  # 48 hours in minutes

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_aws_cli() {
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI not found. Please install AWS CLI first."
        exit 1
    fi
    
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS CLI not configured. Please run 'aws configure' first."
        exit 1
    fi
    
    log_success "AWS CLI is configured"
}

check_script_exists() {
    if [ ! -f "$LOCAL_SCRIPT_PATH" ]; then
        log_error "Script not found: $LOCAL_SCRIPT_PATH"
        exit 1
    fi
    log_success "ETL script found: $LOCAL_SCRIPT_PATH"
}

get_account_id() {
    aws sts get-caller-identity --query Account --output text
}

get_region() {
    echo "$AWS_REGION"
}

upload_script() {
    local s3_bucket="$1"
    local s3_path="s3://${s3_bucket}/${SCRIPT_NAME}"
    
    log_info "Uploading script to S3: $s3_path"
    
    if aws s3 cp "$LOCAL_SCRIPT_PATH" "$s3_path"; then
        log_success "Script uploaded successfully"
        echo "$s3_path"
    else
        log_error "Failed to upload script to S3"
        exit 1
    fi
}

check_iam_role() {
    local account_id="$1"
    local role_arn="arn:aws:iam::${account_id}:role/${GLUE_ROLE_NAME}"
    
    if aws iam get-role --role-name "$GLUE_ROLE_NAME" &> /dev/null; then
        log_success "IAM role exists: $GLUE_ROLE_NAME"
        echo "$role_arn"
    else
        log_error "IAM role not found: $GLUE_ROLE_NAME"
        log_info "Please create the role with the following policies:"
        log_info "- AmazonS3FullAccess (or custom S3 policy)"
        log_info "- service-role/AWSGlueServiceRole"
        exit 1
    fi
}

create_glue_job() {
    local script_location="$1"
    local role_arn="$2"
    
    log_info "Creating Glue job: $JOB_NAME"
    log_info "Installing additional Python modules: pyarrow==$PYARROW_VERSION"
    
    # Check if job already exists
    if aws glue get-job --region "$AWS_REGION" --job-name "$JOB_NAME" &> /dev/null; then
        log_warning "Job already exists: $JOB_NAME"
        read -p "Do you want to update it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            update_glue_job "$script_location" "$role_arn"
        else
            log_info "Skipping job creation"
        fi
        return
    fi
    
    aws glue create-job \
        --region "$AWS_REGION" \
        --job-name "$JOB_NAME" \
        --role "$role_arn" \
        --command Name=pythonshell,ScriptLocation="$script_location",PythonVersion=3.9 \
        --additional-python-modules "pyarrow==$PYARROW_VERSION" \
        --default-arguments '{"--job-bookmark-option":"job-bookmark-disable"}' \
        --max-capacity "$MAX_CAPACITY" \
        --timeout "$TIMEOUT" \
        --description "ETL job to convert S3 CSV files to partitioned Parquet format with timezone conversion and trading hours filtering"
    
    log_success "Glue job created: $JOB_NAME"
}

update_glue_job() {
    local script_location="$1"
    local role_arn="$2"
    
    log_info "Updating Glue job: $JOB_NAME"
    log_info "Installing additional Python modules: pyarrow==$PYARROW_VERSION"
    
    aws glue update-job \
        --region "$AWS_REGION" \
        --job-name "$JOB_NAME" \
        --job-update Role="$role_arn",Command='{Name=pythonshell,ScriptLocation='$script_location',PythonVersion=3.9}',DefaultArguments='{"--job-bookmark-option":"job-bookmark-disable"}',MaxCapacity="$MAX_CAPACITY",Timeout="$TIMEOUT",AdditionalPythonModules="pyarrow==$PYARROW_VERSION"
    
    log_success "Glue job updated: $JOB_NAME"
}

run_glue_job() {
    local year="$1"
    
    if [ -z "$year" ]; then
        log_error "Year parameter is required for job execution"
        echo "Usage: $0 run <YEAR>"
        exit 1
    fi
    
    log_info "Starting Glue job: $JOB_NAME for year $year"
    
    local job_run_id=$(aws glue start-job-run \
        --region "$AWS_REGION" \
        --job-name "$JOB_NAME" \
        --arguments --YEAR="$year" \
        --query JobRunId --output text)
    
    if [ $? -eq 0 ]; then
        log_success "Job started successfully"
        log_info "Job Run ID: $job_run_id"
        log_info "Monitor progress with: aws glue get-job-run --job-name $JOB_NAME --run-id $job_run_id"
        
        # Optionally monitor the job
        read -p "Do you want to monitor the job progress? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            monitor_job "$job_run_id"
        fi
    else
        log_error "Failed to start job"
        exit 1
    fi
}

monitor_job() {
    local job_run_id="$1"
    
    log_info "Monitoring job progress (Ctrl+C to stop monitoring)..."
    
    while true; do
        local status=$(aws glue get-job-run \
            --region "$AWS_REGION" \
            --job-name "$JOB_NAME" \
            --run-id "$job_run_id" \
            --query JobRun.JobRunState --output text)
        
        case "$status" in
            "RUNNING")
                echo -n "."
                ;;
            "SUCCEEDED")
                echo
                log_success "Job completed successfully!"
                break
                ;;
            "FAILED"|"ERROR"|"TIMEOUT")
                echo
                log_error "Job failed with status: $status"
                
                # Get error details
                local error_string=$(aws glue get-job-run \
                    --region "$AWS_REGION" \
                    --job-name "$JOB_NAME" \
                    --run-id "$job_run_id" \
                    --query JobRun.ErrorMessage --output text)
                
                if [ "$error_string" != "None" ]; then
                    log_error "Error: $error_string"
                fi
                break
                ;;
            *)
                echo -n "[$status]"
                ;;
        esac
        
        sleep 10
    done
}

list_jobs() {
    log_info "Listing Glue jobs in $AWS_REGION..."
    aws glue get-jobs --region "$AWS_REGION" --query 'Jobs[?contains(Name, `csv-to-parquet`)].{Name:Name,Role:Role,MaxCapacity:MaxCapacity}' --output table
}

delete_job() {
    log_warning "This will delete the Glue job: $JOB_NAME"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        aws glue delete-job --region "$AWS_REGION" --job-name "$JOB_NAME"
        log_success "Job deleted: $JOB_NAME"
    else
        log_info "Job deletion cancelled"
    fi
}

show_usage() {
    echo "AWS Glue ETL Deployment and Execution Script"
    echo
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo
    echo "Commands:"
    echo "  deploy [S3_BUCKET]     Deploy the ETL script to AWS Glue (uses default if not provided)"
    echo "  run <YEAR>             Execute the ETL job for a specific year"
    echo "  monitor <JOB_RUN_ID>   Monitor a running job"
    echo "  list                   List existing Glue jobs"
    echo "  delete                 Delete the Glue job"
    echo "  help                   Show this help message"
    echo
    echo "Configuration:"
    echo "  AWS_REGION: $AWS_REGION"
    echo "  DEFAULT_S3_BUCKET: $DEFAULT_S3_BUCKET"
    echo
    echo "Examples:"
    echo "  $0 deploy my-glue-scripts-bucket"
    echo "  $0 deploy  # Uses default S3 bucket"
    echo "  $0 run 2024"
    echo "  $0 monitor jr_1234567890abcdef"
    echo
    echo "Environment Variables:"
    echo "  GLUE_ROLE_NAME         IAM role name (default: AWSGlueServiceRole)"
    echo "  JOB_NAME               Glue job name (default: csv-to-parquet-etl)"
}

# Main script logic
main() {
    local command="$1"
    
    case "$command" in
        "deploy")
            local s3_bucket="${2:-$DEFAULT_S3_BUCKET}"
            if [ "$s3_bucket" = "your-glue-scripts-bucket" ]; then
                log_warning "Using default S3 bucket name: $s3_bucket"
                log_warning "Please update DEFAULT_S3_BUCKET in the script or provide a bucket name"
                log_info "Usage: $0 deploy <S3_BUCKET>"
                exit 1
            fi
            
            check_aws_cli
            check_script_exists
            
            log_info "Deploying to AWS region: $AWS_REGION"
            log_info "Using S3 bucket: $s3_bucket"
            
            local account_id=$(get_account_id)
            local role_arn=$(check_iam_role "$account_id")
            local script_location=$(upload_script "$s3_bucket")
            
            create_glue_job "$script_location" "$role_arn"
            ;;
        "run")
            local year="$2"
            check_aws_cli
            log_info "Running job in AWS region: $AWS_REGION"
            run_glue_job "$year"
            ;;
        "monitor")
            local job_run_id="$2"
            if [ -z "$job_run_id" ]; then
                log_error "Job Run ID is required"
                echo "Usage: $0 monitor <JOB_RUN_ID>"
                exit 1
            fi
            check_aws_cli
            monitor_job "$job_run_id"
            ;;
        "list")
            check_aws_cli
            list_jobs
            ;;
        "delete")
            check_aws_cli
            delete_job
            ;;
        "help"|"--help"|"-h"|"")
            show_usage
            ;;
        *)
            log_error "Unknown command: $command"
            show_usage
            exit 1
            ;;
    esac
}

# Execute main function with all arguments
main "$@"