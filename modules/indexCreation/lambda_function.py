import os, boto3, subprocess, tempfile, json

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')

S3_BUCKET = os.environ['BUCKET']         
TARGET_SCAN_QUEUE = os.environ['QUEUE']  
INDEXER_TABLE_NAME = os.environ['INDEXER_TABLE']
BINARY_PATH = "/opt/ISSL/isslCreateIndex"

SEQ_LENGTH = "20"
SLICE_WIDTH = "8"

INDEXER_TABLE = dynamodb.Table(INDEXER_TABLE_NAME)

# --- Helpers ---
def extract_job_id_from_event(event):
    # Expect S3 key: merged/{accession}/{jobid}.offtargets
    record = event['Records'][0]
    s3_key = record['s3']['object']['key']
    job_id = os.path.splitext(os.path.basename(s3_key))[0]
    return job_id, s3_key

def fetch_job_metadata(job_id):
    # Query DynamoDB IndexerTable
    resp = INDEXER_TABLE.get_item(Key={"jobID": job_id})
    if 'Item' not in resp:
        raise ValueError(f"JobID {job_id} not found in IndexerTable")
    item = resp['Item']
    return item['Genome'], item['Sequence'], item['jobID']  # accession, sequence, jobid

def download_offtargets(s3_key, local_path):
    print(f"[INDEX CREATOR] ⬇️ Downloading {s3_key}")
    s3.download_file(S3_BUCKET, s3_key, local_path)
    print(f"[INDEX CREATOR] 🟢 Downloaded to {local_path}")

def run_issl_index(input_path, output_path):
    print(f"[INDEX CREATOR] ⚙️ Running isslCreateIndex...")
    cmd = f"{BINARY_PATH} {input_path} {SEQ_LENGTH} {SLICE_WIDTH} {output_path}"
    ret = os.system(cmd)
    if ret != 0:
        raise RuntimeError(f"isslCreateIndex failed with exit code {ret}")
    print(f"[INDEX CREATOR] 🟢 Binary finished, output at {output_path}")

def upload_index(accession, local_path):
    s3_key = f"{accession}/issl/{accession}.issl"
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"[INDEX CREATOR] 🟢 Uploaded index to {s3_key}")
    return s3_key

def send_completion_message(accession, job_id, sequence):
    body = {
        "Genome": accession,
        "Sequence": sequence,
        "JobID": job_id
    }
    sqs.send_message(
        QueueUrl=TARGET_SCAN_QUEUE,
        MessageBody=json.dumps(body)
    )
    print(f"[INDEX CREATOR] 📩 Sent completion message: {body}")

# --- Lambda Handler ---
def lambda_handler(event, context):
    print("[INDEX CREATOR] 🟠 Received S3 Event:", event)
    
    try:
        job_id, s3_key = extract_job_id_from_event(event)
        accession, sequence, job_id = fetch_job_metadata(job_id)
        print(f"[INDEX CREATOR] 🆔 Accession: {accession}, JobID: {job_id}, Sequence: {sequence}")
    except ValueError as e:
        print(f"[INDEX CREATOR] 🔴 {e}")
        return {"statusCode": 400, "body": str(e)}

    with tempfile.TemporaryDirectory() as tmpdir:
        input_path = os.path.join(tmpdir, "input.offtargets")
        output_path = os.path.join(tmpdir, "output.issl")

        # 1. Download merged off-targets
        download_offtargets(s3_key, input_path)

        # 2. Run ISSL binary
        run_issl_index(input_path, output_path)

        # 3. Upload index
        s3_output_key = upload_index(accession, output_path)

        # 4. Optionally delete merged off-targets
        s3.delete_object(Bucket=S3_BUCKET, Key=s3_key)
        print(f"[INDEX CREATOR] 🗑️ Deleted merged off-targets: {s3_key}")

        # 5. Send completed message
        send_completion_message(accession, job_id, sequence)

        # 6. Delete job from IndexerTable
        INDEXER_TABLE.delete_item(Key={"jobID": job_id})
        print(f"[INDEX CREATOR] 🗑️ Deleted job {job_id} from IndexerTable")

    return {
        "statusCode": 200,
        "body": f"ISSL index uploaded to {s3_output_key} and completion message sent"
    }
