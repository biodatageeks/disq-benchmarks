# Disq Benchmarks

Benchmarks for [Disq](https://github.com/tomwhite/disq).

## Results

Running count reads on a 136.78 GiB [BAM file](ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/NA12878/NA12878_PacBio_MtSinai/sorted_final_merged.bam)
in Google Cloud Storage (GCS). Total reads: 134217728.

| Library    | Filesystem connector | Time (s) |
| ---------- | -------------------- | -------- |
| Disq       | GCS Connector        | 144      |
| Disq       | NIO                  | 277      |
| Hadoop-BAM | GCS Connector        | 278      |
| Spark-BAM  | NIO                  | 273      |

## Running

The _download.sh_ script is for retrieving the source data and storing in the
cloud. The paths will need changing if you want to store the data in a
different bucket.

The _run.sh_ script assembles the benchmarking code and runs the commands.
Some of the variables may need changing for your environment.

Timing results are written to _results/run.csv_, except for Spark-BAM which
writes to the console.