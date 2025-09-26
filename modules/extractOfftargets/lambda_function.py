# Imports
import boto3, os, tempfile, shutil, json
from boto3.dynamodb.conditions import Key

import extractOfftargets

# Environment variables
BUCKET_NAME = os.environ['BUCKET']
INDEXER_TABLE_NAME = os.environ['INDEXER_TABLE']
MERGER_FUNCTION_NAME = os.environ["MERGER_FUNCTION"]

# AWS clients
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')
lambda_client = boto3.client("lambda")
dynamodb = boto3.resource('dynamodb')
INDEXER_TABLE = dynamodb.Table(INDEXER_TABLE_NAME)

# --- Helper Functions ---
def parse_s3_key(s3_key: str):
    # Expected format: chunks/<accession>/chunk_X.fasta
    parts = s3_key.split("/")
    if len(parts) != 3 or parts[0] != "chunks" or not s3_key.endswith(".fasta"):
        return None, None
    accession = parts[1]  # second element is the genome accession
    chunk_file = parts[2]  # third element is the chunk file
    return accession, chunk_file


def download_chunk(accession, chunk_file):
    # Download chunk from S3 to temp
    local_path = f"/tmp/{chunk_file}"
    s3_key = f"chunks/{accession}/{chunk_file}"
    s3_client.download_file(BUCKET_NAME, s3_key, local_path)
    return local_path


def run_extract_offtargets(input_path, chunk_file):
    # Runs extract off targets on chunk
    chunk_name = os.path.splitext(chunk_file)[0]
    output_path = f"/tmp/{chunk_name}.offtargets"
    temp_dir = tempfile.mkdtemp()

    extractOfftargets.startSequentialProcessing(
        fpInputs=[input_path],
        fpOutput=output_path,
        numThreads=1,
        maxOpenFiles=100,
        temp_dir=temp_dir,
    )

    return output_path, temp_dir, chunk_name


def upload_offtargets(accession, chunk_name, output_path):
    # Upload offtargets file to S3
    s3_output_key = f"offtargets/{accession}/{chunk_name}.offtargets"
    with open(output_path, "rb") as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, s3_output_key)
    print(f"[OFFTARGET EXTRACTOR] 🟢 Uploaded {s3_output_key}")

    return s3_output_key


def get_jobid_from_accession(accession):
    response = INDEXER_TABLE.query(
        IndexName="Genome-index",   # you need a GSI on Genome attribute
        KeyConditionExpression=Key("Genome").eq(accession),
        Limit=1
    )
    if response["Items"]:
        return response["Items"][0]["jobID"]
    else:
        raise ValueError(f"No job found for accession {accession}")



def update_progress(jobid, accession):
    # Increment completed chunks in dynamoDB
    update_response = INDEXER_TABLE.update_item(
        Key={"jobID": jobid},
        UpdateExpression="SET completedChunks = completedChunks + :inc",
        ExpressionAttributeValues={":inc": 1},
        ReturnValues="UPDATED_NEW",
    )

    completed = update_response["Attributes"]["completedChunks"]

    # Fetch totalChunks
    item = INDEXER_TABLE.get_item(Key={"jobID": jobid})
    total = item["Item"]["totalChunks"]

    return completed, total


def maybe_trigger_merger(jobid, accession, completed, total):
    # Trigger merger Lambda if all chunks are done
    if completed == total:
        print(f"[OFFTARGET EXTRACTOR] 🟢 All chunks processed for job {jobid}. Triggering merger...")
        lambda_client.invoke(
            FunctionName=MERGER_FUNCTION_NAME,
            InvocationType="Event",
            Payload=json.dumps(
                {
                    "jobID": jobid,
                    "Genome": accession,
                    "total_chunks": int(total),
                }
            ),
        )


# --- Lambda Handler ---
def lambda_handler(event, context):
    for record in event["Records"]:
        s3_key = record["s3"]["object"]["key"]

        accession, chunk_file = parse_s3_key(s3_key)
        

        if not accession:
            continue

        jobid = get_jobid_from_accession(accession)
        print(f"[OFFTARGET EXTRACTOR] Processing {chunk_file} for job {jobid}")

        input_path = None
        output_path = None
        temp_dir = None

        try:
            # Step 1: Download chunk
            input_path = download_chunk(accession, chunk_file)

            # Step 2: Run extractOfftargets
            output_path, temp_dir, chunk_name = run_extract_offtargets(
                input_path, chunk_file
            )

            # Step 3: Upload results
            upload_offtargets(accession, chunk_name, output_path)

            # Step 4: Update progress
            completed, total = update_progress(jobid, accession)

            # Step 5: Trigger merger if done
            maybe_trigger_merger(jobid, accession, completed, total)

        except Exception as e:
            print(f"[OFFTARGET EXTRACTOR] 🔴 Error processing {chunk_file}: {str(e)}")
            raise

        finally:
            # Cleanup
            for f in [input_path, output_path]:
                try:
                    if f and os.path.exists(f):
                        os.remove(f)
                except Exception as cleanup_err:
                    print(f"Cleanup error: {str(cleanup_err)}")
            if temp_dir and os.path.exists(temp_dir):
                shutil.rmtree(temp_dir, ignore_errors=True)

    return {"status": "ok"}
