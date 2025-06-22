#!/bin/bash

# AWS Glue ETL Deployment and Execution Script
# Usage: ./shell_scripts/deploy_glue_etl.sh [COMMAND] [OPTIONS]

set -e  # Exit on any error

# Configuration
AWS_REGION="us-west-2"
DEFAULT_S3_BUCKET="ap-glue-scripts-bucket"
DEFAULT_SHELL_SCRIPT="s3_csv_to_s3_parquet_shell_job.py"
DEFAULT_SPARK_SCRIPT="s3_csv_to_s3_parquet_spark_job.py"
GLUE_ROLE_NAME="AWSGlueServiceRole-Default"

# Job type specific configurations
SHELL_JOB_CONFIG=(
    "MAX_CAPACITY=1.0"
    "TIMEOUT=2880"
    "WORKER_TYPE="
    "NUMBER_OF_WORKERS="
)

SPARK_JOB_CONFIG=(
    "MAX_CAPACITY="
    "TIMEOUT=2880"
    "WORKER_TYPE=G.1X"
    "NUMBER_OF_WORKERS=2"
)

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Job type detection and configuration
detect_job_type() {
    local script_name="$1"
    local manual_type="$2"
    
    # Manual override takes precedence
    if [ -n "$manual_type" ]; then
        case "$manual_type" in
            "spark"|"glueetl")
                echo "spark"
                return
                ;;
            "shell"|"pythonshell")
                echo "shell"
                return
                ;;
            *)
                log_error "Invalid job type: $manual_type. Use 'spark' or 'shell'"
                exit 1
                ;;
        esac
    fi
    
    # Auto-detect based on script name
    if [[ "$script_name" == *"spark"* ]]; then
        echo "spark"
    elif [[ "$script_name" == *"shell"* ]]; then
        echo "shell"
    else
        # Default to shell for backward compatibility
        log_warning "Could not auto-detect job type from script name: $script_name"
        log_warning "Defaulting to Python Shell job. Use --job-type to override."
        echo "shell"
    fi
}

set_job_config() {
    local job_type="$1"
    
    if [ "$job_type" = "spark" ]; then
        # Load Spark job configuration
        for config in "${SPARK_JOB_CONFIG[@]}"; do
            export "$config"
        done
        COMMAND_TYPE="glueetl"
        JOB_TYPE_DISPLAY="Spark (glueetl)"
    else
        # Load Shell job configuration
        for config in "${SHELL_JOB_CONFIG[@]}"; do
            export "$config"
        done
        COMMAND_TYPE="pythonshell"
        JOB_TYPE_DISPLAY="Python Shell (pythonshell)"
    fi
}

show_job_info() {
    local script_name="$1"
    local job_name="$2"
    local job_type="$3"
    
    log_info "=== JOB CONFIGURATION ==="
    log_info "Script: $script_name"
    log_info "Job Name: $job_name"
    log_info "Job Type: $JOB_TYPE_DISPLAY"
    log_info "Command Type: $COMMAND_TYPE"
    
    if [ "$job_type" = "spark" ]; then
        log_info "Worker Type: $WORKER_TYPE"
        log_info "Number of Workers: $NUMBER_OF_WORKERS"
    else
        log_info "Max Capacity: $MAX_CAPACITY DPU"
    fi
    
    log_info "Timeout: $TIMEOUT minutes"
    log_info "AWS Region: $AWS_REGION"
    log_info "S3 Bucket: $S3_BUCKET"
    log_info "=========================="
}

list_available_scripts() {
    log_info "Available ETL scripts in etl/ directory:"
    if [ -d "etl" ]; then
        for script in etl/*.py; do
            if [ -f "$script" ]; then
                script_basename=$(basename "$script")
                if [[ "$script_basename" == *"spark"* ]]; then
                    echo "  ✓ $script_basename (Spark job)"
                elif [[ "$script_basename" == *"shell"* ]]; then
                    echo "  ✓ $script_basename (Python Shell job)"
                else
                    echo "  ? $script_basename (Unknown type - will default to Shell)"
                fi
            fi
        done
    else
        log_warning "etl/ directory not found"
    fi
}

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
    
    log_info "Uploading script to S3: $s3_path" >&2
    
    if aws s3 cp "$LOCAL_SCRIPT_PATH" "$s3_path" >&2; then
        log_success "Script uploaded successfully" >&2
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
        log_success "IAM role exists: $GLUE_ROLE_NAME" >&2
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
    local job_type="$3"
    
    log_info "Creating Glue job: $JOB_NAME ($JOB_TYPE_DISPLAY)"
    
    # Check if job already exists
    if aws glue get-job --region "$AWS_REGION" --job-name "$JOB_NAME" &> /dev/null; then
        log_warning "Job already exists: $JOB_NAME"
        read -p "Do you want to update it? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            update_glue_job "$script_location" "$role_arn" "$job_type"
        else
            log_info "Skipping job creation"
        fi
        return
    fi
    
    # Build command based on job type
    if [ "$job_type" = "spark" ]; then
        # Spark job configuration
        aws glue create-job \
            --region "$AWS_REGION" \
            --name "$JOB_NAME" \
            --role "$role_arn" \
            --command Name="$COMMAND_TYPE",ScriptLocation="$script_location" \
            --default-arguments "{\"--job-bookmark-option\":\"job-bookmark-disable\",\"--enable-continuous-cloudwatch-log\":\"true\",\"--enable-metrics\":\"\"}" \
            --worker-type "$WORKER_TYPE" \
            --number-of-workers "$NUMBER_OF_WORKERS" \
            --timeout "$TIMEOUT" \
            --description "Spark ETL job to convert S3 CSV files to partitioned Parquet format with distributed processing"
    else
        # Python Shell job configuration  
        aws glue create-job \
            --region "$AWS_REGION" \
            --name "$JOB_NAME" \
            --role "$role_arn" \
            --command Name="$COMMAND_TYPE",ScriptLocation="$script_location",PythonVersion=3.9 \
            --default-arguments "{\"--job-bookmark-option\":\"job-bookmark-disable\",\"--enable-continuous-cloudwatch-log\":\"true\",\"--enable-metrics\":\"\"}" \
            --max-capacity "$MAX_CAPACITY" \
            --timeout "$TIMEOUT" \
            --description "Python Shell ETL job to convert S3 CSV files to partitioned Parquet format"
    fi
    
    log_success "Glue job created: $JOB_NAME ($JOB_TYPE_DISPLAY)"
}

update_glue_job() {
    local script_location="$1"
    local role_arn="$2"
    local job_type="$3"
    
    log_info "Updating Glue job: $JOB_NAME ($JOB_TYPE_DISPLAY)"
    
    if [ "$job_type" = "spark" ]; then
        # Spark job update
        aws glue update-job \
            --region "$AWS_REGION" \
            --job-name "$JOB_NAME" \
            --job-update Role="$role_arn",Command="{Name=$COMMAND_TYPE,ScriptLocation=$script_location}",DefaultArguments='{"--job-bookmark-option":"job-bookmark-disable","--enable-continuous-cloudwatch-log":"true","--enable-metrics":""}',WorkerType="$WORKER_TYPE",NumberOfWorkers="$NUMBER_OF_WORKERS",Timeout="$TIMEOUT"
    else
        # Python Shell job update
        aws glue update-job \
            --region "$AWS_REGION" \
            --job-name "$JOB_NAME" \
            --job-update Role="$role_arn",Command="{Name=$COMMAND_TYPE,ScriptLocation=$script_location,PythonVersion=3.9}",DefaultArguments='{"--job-bookmark-option":"job-bookmark-disable","--enable-continuous-cloudwatch-log":"true","--enable-metrics":""}',MaxCapacity="$MAX_CAPACITY",Timeout="$TIMEOUT"
    fi
    
    log_success "Glue job updated: $JOB_NAME ($JOB_TYPE_DISPLAY)"
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
        --arguments "{\"--YEAR\":\"$year\"}" \
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
    echo "Enhanced AWS Glue ETL Deployment and Execution Script"
    echo "Supports both Python Shell and Spark jobs with auto-detection"
    echo
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo
    echo "Commands:"
    echo "  deploy [SCRIPT] [--options]    Deploy ETL script to AWS Glue"
    echo "  run <YEAR> [JOB_NAME]          Execute ETL job for specific year"
    echo "  monitor <JOB_RUN_ID>           Monitor a running job"
    echo "  list                           List existing Glue jobs"
    echo "  delete [JOB_NAME]              Delete the specified Glue job"
    echo "  help                           Show this help message"
    echo
    echo "Deploy Options:"
    echo "  --job-type spark|shell         Override auto-detected job type"
    echo "  --bucket BUCKET_NAME           Specify S3 bucket (default: $DEFAULT_S3_BUCKET)"
    echo "  --list-scripts                 List available scripts with detected types"
    echo "  --help, -h                     Show deploy help"
    echo
    echo "Auto-Detection Rules:"
    echo "  Scripts with 'spark' in name   → Spark job (glueetl)"
    echo "  Scripts with 'shell' in name   → Python Shell job (pythonshell)"
    echo "  Other scripts                  → Default to Python Shell"
    echo
    echo "Job Type Configurations:"
    echo "  Python Shell: 1 DPU, 2880min timeout"
    echo "  Spark:        G.1X workers (2), 2880min timeout"
    echo
    echo "Examples:"
    echo "  # Auto-detect job type from script name:"
    echo "  $0 deploy s3_csv_to_s3_parquet_spark_job.py    # → Spark job"
    echo "  $0 deploy s3_csv_to_s3_parquet_shell_job.py    # → Python Shell job"
    echo
    echo "  # Manual job type override:"
    echo "  $0 deploy my_script.py --job-type spark"
    echo
    echo "  # Custom bucket:"
    echo "  $0 deploy my_script.py --bucket my-custom-bucket"
    echo
    echo "  # Run jobs:"
    echo "  $0 run 2025 s3-csv-to-s3-parquet-spark-job"
    echo "  $0 run 2025                                     # Uses default job"
    echo
    echo "  # List available scripts:"
    echo "  $0 deploy --list-scripts"
    echo
    echo "Configuration:"
    echo "  AWS_REGION: $AWS_REGION"
    echo "  DEFAULT_S3_BUCKET: $DEFAULT_S3_BUCKET"
    echo "  GLUE_ROLE_NAME: $GLUE_ROLE_NAME"
}

# Main script logic
main() {
    local command="$1"
    
    case "$command" in
        "deploy")
            # Parse arguments with support for script name and job type override
            local s3_bucket="$DEFAULT_S3_BUCKET"
            local script_name=""
            local manual_job_type=""
            
            # Parse named and positional arguments
            shift # Remove 'deploy' command
            while [[ $# -gt 0 ]]; do
                case $1 in
                    --job-type)
                        manual_job_type="$2"
                        shift 2
                        ;;
                    --bucket)
                        s3_bucket="$2"
                        shift 2
                        ;;
                    --help|-h)
                        show_usage
                        exit 0
                        ;;
                    --list-scripts)
                        list_available_scripts
                        exit 0
                        ;;
                    *)
                        if [ -z "$script_name" ] && [[ "$1" == *.py ]]; then
                            script_name="$1"
                        else
                            log_error "Unknown argument: $1"
                            show_usage
                            exit 1
                        fi
                        shift
                        ;;
                esac
            done
            
            # Set default script if none provided
            if [ -z "$script_name" ]; then
                script_name="$DEFAULT_SHELL_SCRIPT"
                log_info "No script specified, using default: $script_name"
            fi
            
            # Auto-detect job type
            local job_type=$(detect_job_type "$script_name" "$manual_job_type")
            
            # Set job configuration based on type
            set_job_config "$job_type"
            
            # Set global variables
            SCRIPT_NAME="$script_name"
            LOCAL_SCRIPT_PATH="etl/${SCRIPT_NAME}"
            S3_BUCKET="$s3_bucket"
            
            # Generate job name from script
            JOB_NAME=$(echo "$script_name" | sed 's/\.py$//' | sed 's/_/-/g')
            
            # Show configuration
            show_job_info "$script_name" "$JOB_NAME" "$job_type"
            
            # Validate and deploy
            check_aws_cli
            check_script_exists
            
            local account_id=$(get_account_id)
            local role_arn=$(check_iam_role "$account_id")
            local script_location=$(upload_script "$s3_bucket")
            
            create_glue_job "$script_location" "$role_arn" "$job_type"
            ;;
        "run")
            local year="$2"
            local job_name="$3"
            
            if [ -z "$year" ]; then
                log_error "Year parameter is required"
                echo "Usage: $0 run <YEAR> [JOB_NAME]"
                exit 1
            fi
            
            # Set job name (default if not provided)
            if [ -n "$job_name" ]; then
                JOB_NAME="$job_name"
            else
                JOB_NAME="s3-csv-to-s3-parquet-shell-job"  # Default job name
            fi
            
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
            local job_name="$2"
            
            # Set job name (default if not provided)
            if [ -n "$job_name" ]; then
                JOB_NAME="$job_name"
            else
                JOB_NAME="s3-csv-to-s3-parquet-shell-job"  # Default job name
            fi
            
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