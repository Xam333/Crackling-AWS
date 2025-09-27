# Imports
import os
import boto3
import heapq
import io
import tempfile

s3 = boto3.client("s3")
S3_BUCKET = os.environ['BUCKET']

RECORD40_SIZE = 5  # 40-bit input records (seq only)
RECORD64_SIZE = 8  # 64-bit output records (seq + count)


# ------------------------------
# Binary I/O Helpers
# ------------------------------
def read_record_40(f):
    """Read one 40-bit record (seq only)."""
    data = f.read(RECORD40_SIZE)
    if not data or len(data) < RECORD40_SIZE:
        return None
    return int.from_bytes(data, "big")


def write_record_64(f, seq, count):
    """Write 64-bit record (seq + 24-bit count)."""
    packed = (seq << 24) | (count & 0xFFFFFF)
    f.write(packed.to_bytes(RECORD64_SIZE, "big"))


# ------------------------------
# S3 Helpers
# ------------------------------
def download_chunk_files(accession, total_chunks):
    """Download all chunk .offtargets files for this accession into /tmp."""
    local_files = []

    for i in range(total_chunks):
        chunk_key = f"offtargets/{accession}/chunk_{i}.offtargets"
        local_path = os.path.join(tempfile.gettempdir(), f"chunk_{i}.offtargets")
        try:
            s3.download_file(S3_BUCKET, chunk_key, local_path)
            local_files.append(local_path)
            print(f"[MERGER] ⬇️ Downloaded {chunk_key}")
        except Exception as e:
            print(f"[MERGER] 🔴 Could Not Download {chunk_key}: {e}")

    return local_files


def upload_merged_file(accession, job_id, merged_stream):
    merged_key = f"merged/{accession}/{job_id}.offtargets"
    merged_stream.seek(0)
    s3.upload_fileobj(merged_stream, S3_BUCKET, merged_key)
    print(f"[MERGER] 🟢 Uploaded merged file: {merged_key}")
    return merged_key


def cleanup_chunks(accession, total_chunks):
    """Delete per-chunk .offtargets and .fasta files from S3."""
    print(f"[MERGER] 🟠 Cleaning up chunk files from S3...")
    for i in range(total_chunks):
        offtarget_chunk_key = f"offtargets/{accession}/chunk_{i}.offtargets"
        fasta_chunk_key = f"chunks/{accession}/chunk_{i}.fasta"
        for key in [offtarget_chunk_key, fasta_chunk_key]:
            try:
                s3.delete_object(Bucket=S3_BUCKET, Key=key)
                print(f"[MERGER] 🗑️ Deleted {key}")
            except Exception as e:
                print(f"[MERGER] 🔴 Failed to delete {key}: {e}")


# ------------------------------
# Merge Logic
# ------------------------------
def merge_offtargets(local_files):
    """Perform k-way merge of sorted .offtargets files into 64-bit stream."""
    def make_iter(f):
        while True:
            seq = read_record_40(f)
            if seq is None:
                break
            yield (seq, 1)  # each entry counts as 1
        f.close()

    file_iters = []
    for path in local_files:
        f = open(path, "rb")
        file_iters.append(make_iter(f))

    merged_stream = io.BytesIO()
    merged_iter = heapq.merge(*file_iters, key=lambda x: x[0])

    prev_seq, total_count = None, 0
    for seq, count in merged_iter:
        if seq == prev_seq:
            total_count += count
        else:
            if prev_seq is not None:
                write_record_64(merged_stream, prev_seq, total_count)
            prev_seq, total_count = seq, count

    if prev_seq is not None:
        write_record_64(merged_stream, prev_seq, total_count)

    return merged_stream


# ------------------------------
# Lambda Handler
# ------------------------------
def lambda_handler(event, context):
    print(f"[MERGER] 🟠 Received Event: {event}")

    job_id = event.get("jobID")       # DynamoDB job ID
    accession = event.get("Genome")   # accession string
    total_chunks_raw = event.get("total_chunks")

    if not job_id or not accession or total_chunks_raw is None:
        return {"statusCode": 400, "body": "Missing jobID, Genome, or total_chunks"}

    try:
        total_chunks = int(total_chunks_raw)
    except ValueError:
        return {"statusCode": 400, "body": "Invalid value for total_chunks"}

    print(f"[MERGER] 🟠 Starting merge for job {job_id}, accession {accession} ({total_chunks} chunks)")

    # 1. Download per-chunk files
    local_files = download_chunk_files(accession, total_chunks)

    # 2. Merge into 64-bit records
    merged_stream = merge_offtargets(local_files)

    # 3. Upload merged file
    merged_key = upload_merged_file(accession, job_id, merged_stream)

    # 4. Cleanup S3 chunks
    cleanup_chunks(accession, total_chunks)

    print(f"[MERGER] ✅ Completed merge for job {job_id}, accession {accession}")

    return {
        "statusCode": 200,
        "body": f"✅ Merged {total_chunks} chunks for {accession} into {merged_key} and deleted source chunks",
    }
