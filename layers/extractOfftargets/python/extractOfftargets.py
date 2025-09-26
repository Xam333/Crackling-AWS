import glob, os, re, shutil, sys, tempfile, heapq, argparse

# Regex patterns for target sites
pattern_forward_offsite = r"(?=([ACG][ACGT]{19}[ACGT][AG]G))"
pattern_reverse_offsite = r"(?=(C[CT][ACGT][ACGT]{19}[TGC]))"

def rc(dna):
    complements = str.maketrans('ACGT', 'TGCA')
    return dna.translate(complements)[::-1]

# Encode DNA sequence (20bp) into 40 bits
def encode_seq_40bits(seq: str) -> int:
    mapping = {'A': 0, 'C': 1, 'G': 2, 'T': 3}
    val = 0
    # reversed so first base is in least-significant 2 bits
    for base in reversed(seq):
        val = (val << 2) | mapping[base]
    return val

# Write final binary file (40-bit records, packed to 5 bytes)
def write_binary40(sorted_input_fp, binary_output_fp):
    total_seqs = 0
    with open(sorted_input_fp, 'r') as f_in, open(binary_output_fp, 'wb') as f_out:
        for line in f_in:
            seq = line.strip()
            if not seq:
                continue
            val = encode_seq_40bits(seq)
            f_out.write(val.to_bytes(5, "big"))  # 40 bits = 5 bytes
            total_seqs += 1
    return total_seqs

# Extraction: get 20mers and write to temp file
def processingNode(fpInput, fpOutputTempDir=None):
    fpTemp = tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=fpOutputTempDir)
    with open(fpTemp.name, 'w') as outFile, open(fpInput, 'r') as f:
        header, seq_lines = None, []
        for line in f:
            line = line.strip()
            if line.startswith('>'):
                if header:
                    process_sequence(''.join(seq_lines), outFile)
                header, seq_lines = line[1:], []
            else:
                seq_lines.append(line.upper())
        if header:
            process_sequence(''.join(seq_lines), outFile)
    return fpTemp.name

def process_sequence(sequence, outFile):
    for pattern, seqModifier in [
        (pattern_forward_offsite, lambda x: x),
        (pattern_reverse_offsite, rc)
    ]:
        for match in re.findall(pattern, sequence):
            outFile.write(seqModifier(match[:20]) + '\n')

# Sort one file
def sortingNode(fileToSort, sortedTempDir):
    sortedFile = tempfile.NamedTemporaryFile(mode='w+', delete=False, dir=sortedTempDir)
    with open(fileToSort, 'r') as input:
        page = input.readlines()
        page.sort()
    with open(sortedFile.name, 'w') as out:
        out.writelines(page)
    return sortedFile.name

# Merge-sort sorted chunks
def paginatedSort(filesToSort, fpOutput, maxNumOpenFiles=400, temp_dir=None):
    sortedTempDir = os.path.join(temp_dir, "sorted")
    os.makedirs(sortedTempDir, exist_ok=True)
    sortedFiles = [sortingNode(f, sortedTempDir) for f in filesToSort]

    while len(sortedFiles) > 1:
        mergedFile = tempfile.NamedTemporaryFile(delete=False, dir=sortedTempDir)
        sortedFilesPointers = [open(f, 'r') for f in sortedFiles[:maxNumOpenFiles]]
        with open(mergedFile.name, 'w') as f:
            f.writelines(heapq.merge(*sortedFilesPointers))
        for f in sortedFilesPointers:
            f.close()
        sortedFiles = sortedFiles[maxNumOpenFiles:] + [mergedFile.name]

    shutil.move(sortedFiles[0], fpOutput)

# Main pipeline
def startSequentialProcessing(fpInputs, fpOutput, numThreads, maxOpenFiles, temp_dir=None):
    print('Extracting off-targets...')

    if temp_dir is None:
        temp_dir = tempfile.mkdtemp()

    inputFiles = []
    for path in fpInputs:
        if os.path.isdir(path):
            inputFiles.extend(glob.glob(os.path.join(path, '*')))
        else:
            inputFiles.append(path)

    print(f'Processing {len(inputFiles)} FASTA files...')

    temp_outputs = []
    for f in inputFiles:
        temp_outputs.append(processingNode(f, temp_dir))

    print('Sorting results...')
    sorted_fp = os.path.join(temp_dir, "all_sorted.txt")
    paginatedSort(temp_outputs, sorted_fp, maxOpenFiles, temp_dir=temp_dir)

    print('Writing binary 40-bit encoded file...')
    total_written = write_binary40(sorted_fp, fpOutput)

    # 🔎 Print summary stats
    file_size = os.path.getsize(fpOutput)
    print(f'Done. Binary off-targets written to: {fpOutput}')
    print(f'  Total off-targets written: {total_written:,}')
    print(f'  File size: {file_size/1e6:.2f} MB ({file_size} bytes)')


def main():
    parser = argparse.ArgumentParser(description='Extract CRISPR target sites (binary 40-bit format).')
    parser.add_argument('output', help='Binary output file')
    parser.add_argument('inputs', nargs='+', help='FASTA inputs')
    parser.add_argument('--maxOpenFiles', type=int, default=1000)
    parser.add_argument('--threads', type=int, default=os.cpu_count())
    args = parser.parse_args()
    startSequentialProcessing(args.inputs, args.output, args.threads, args.maxOpenFiles)

if __name__ == '__main__':
    main()
