# imports
import sys, os, shutil, boto3, gzip, json, tempfile
from io import BytesIO

# Global variables
s3_bucket = os.environ['BUCKET']
BYTE_TO_MB = 1024 * 1024
CHUNK_SIZE_MB = int(os.environ.get("CHUNK_SIZE_MB", "100")) # Target chunk size (chunks split once they reach this size)                        
CHUNK_SIZE = CHUNK_SIZE_MB * BYTE_TO_MB    # Convert to bytes
MAX_FASTA_SIZE_MB = 3200
MAX_FASTA_SIZE = MAX_FASTA_SIZE_MB * BYTE_TO_MB

# Create S3 client
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Create dynamodb client
dynamodb = boto3.resource('dynamodb')
INDEXER_TABLE_NAME = os.environ['INDEXER_TABLE']
INDEXER_TABLE = dynamodb.Table(INDEXER_TABLE_NAME)

# --- Helper Functions ---
def parse_job_message(record):
    # Extract job metadata from the SQS message
    message = json.loads(record["body"])

    job_data = {
        "jobid": message["JobID"],
        "accession": message["Genome"],
        "sequence": message["Sequence"],
    }

    print(f"[GENOME SPLITTER] 🟢 Loaded Job Data: {job_data}")
    
    return job_data


def fasta_size_check(accession: str, min_size_mb: int = CHUNK_SIZE_MB):
    # Ensure fasta file size is below threshold
    prefix = f"{accession}/fasta/"
    paginator = s3_client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

    total_size = 0
    for page in response_iterator:
        for obj in page.get("Contents", []):
            total_size += obj["Size"]

    if total_size < 1:
        print("[GENOME SPLITTER] 🔴 No FASTA files found for accession:", accession)
        sys.exit("Error - Accession FASTA file is missing.")

    filesize_in_MB = total_size / BYTE_TO_MB
    print(f"FASTA size for {accession}: {filesize_in_MB:.2f} MB")

    if filesize_in_MB > MAX_FASTA_SIZE_MB:
        sys.exit(
            f"Error - Accession FASTA file is too large "
            f"({filesize_in_MB:.2f} MB). Max supported: {MAX_FASTA_SIZE_MB} MB."
        )

    return filesize_in_MB


def s3_multi_file_to_tmp(accession: str):
    # Download files from S3, unzip them to a temp folder
    prefix = f"{accession}/fasta/"
    paginator = s3_client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=prefix)

    # Collect all S3 object keys
    downloaded_files = []
    for page in response_iterator:
        files = [obj["Key"] for obj in page.get("Contents", [])]
        downloaded_files.extend(files)

    if not downloaded_files:
        sys.exit(f"[GENOME SPLITTER] 🔴 No FASTA files found for accession {accession}")

    # Temp directories
    tmp_dir = tempfile.mkdtemp(prefix=f"{accession}_gz_")
    tmp_extract_dir = tempfile.mkdtemp(prefix=f"{accession}_fasta_")
    extracted_files = []

    for s3_key in downloaded_files:
        file_name = os.path.basename(s3_key)
        tmp_gz_file = os.path.join(tmp_dir, file_name)
        tmp_extract_file = os.path.join(tmp_extract_dir, os.path.splitext(file_name)[0])

        print(f"[GENOME SPLITTER] 🔽 Downloading {s3_key} → {tmp_gz_file}")
        s3_client.download_file(s3_bucket, s3_key, tmp_gz_file)

        # Unzip
        with gzip.open(tmp_gz_file, "rb") as f_in, open(tmp_extract_file, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

        extracted_files.append(tmp_extract_file)
        os.remove(tmp_gz_file)
        print(f"[GENOME SPLITTER] 🟢 Extracted and removed {tmp_gz_file}")

    print(f"[GENOME SPLITTER] 🟢 Extracted files: {extracted_files}")
    return extracted_files, tmp_extract_dir
        

def split_and_upload(fasta_file_path, accession):
    chunk_index = 0
    chunk_size = 0
    chunk_lines = []

    with open(fasta_file_path, "rb") as f_in:
        for line in f_in:  # <- standard file iteration, no .iter_lines()
            if not line.strip():
                continue

            chunk_lines.append(line)
            chunk_size += len(line)

            if line.startswith(b">") and chunk_size >= CHUNK_SIZE and chunk_lines:
                # Upload current chunk to S3
                upload_chunk_to_s3(chunk_lines, accession, chunk_index)
                chunk_index += 1
                chunk_lines, chunk_size = [], 0

        # Upload any remaining lines
        if chunk_lines:
            upload_chunk_to_s3(chunk_lines, accession, chunk_index)
            chunk_index += 1

    return chunk_index  # total chunks uploaded


def upload_chunk_to_s3(lines, accession, index):
    s3_destination_path = f"chunks/{accession}/chunk_{index}.fasta"  # removed leading /
    chunk_data = BytesIO(b"".join(lines))
    s3_client.upload_fileobj(chunk_data, s3_bucket, s3_destination_path)
    print(f"[GENOME SPLITTER] 🟢 Uploaded {s3_destination_path}")



def record_job_progress(jobid, accession, sequence, total_chunks):
    # Insert intial indexer job tracking record into DynamoDB
    response_dynamo = INDEXER_TABLE.put_item(
        Item = {
            "jobID": jobid,
            "Genome": accession,
            "Sequence": sequence,
            "totalChunks": total_chunks,
            "completedChunks": 0,
            "status": "processing",
        }
    )
    return response_dynamo


# --- Main Program Workflow --- 
def process_job(job_data):
    # 1. Get job data
    jobid = job_data["jobid"]
    accession = job_data["accession"]
    sequence = job_data.get("sequence")

    print(f"[GENOME SPLITTER] Starting job {jobid} for accession {accession}")

    # 2. Check fasta file size
    fasta_size_check(accession)

    # 3. Download and unzip genome from s3
    extracted_files, tmp_extract_dir = s3_multi_file_to_tmp(accession)

    # 4. Split and upload chunks
    total_chunks = 0
    for fasta_file in extracted_files:
        total_chunks += split_and_upload(fasta_file, accession)


    # 5. Add record to indexer table
    record_job_progress(jobid, accession, sequence, total_chunks)
    print(f"[GENOME SPLITTER] 🟢 Job {jobid} recorded with {total_chunks} chunks")

    # 6. Cleanup temp files
    if os.path.exists(tmp_extract_dir):
        shutil.rmtree(tmp_extract_dir)

    print(f"[GENOME SPLITTER] 🟢 Completed job {jobid}")


# --- Lambda Handler ---
def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            job_data = parse_job_message(record)    # Parse job data from message
            process_job(job_data)
        except Exception as e:
            print(f"[GENOME SPLITTER] 🔴 Error Processing Job Data : {e}")
            continue

    return {"statusCode": 200, "body": "Chunking Complete"}
