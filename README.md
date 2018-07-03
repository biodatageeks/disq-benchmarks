# Disq Benchmarks

Benchmarks for [Disq].

## Results

### Performance

Running count reads on a 136.78 GiB [BAM file](ftp://ftp-trace.ncbi.nlm.nih.gov/giab/ftp/data/NA12878/NA12878_PacBio_MtSinai/sorted_final_merged.bam)
in Google Cloud Storage (GCS). The file contains 68,064,542 reads.

| Filesystem connector     | Library      | Time (s) |
| ------------------------ | ------------ | -------- |
| GCS Connector            | [Disq]       | 144      |
| GCS Connector            | [Hadoop-BAM] | 278      |
| GCS NIO                  | [Disq]       | 277      |
| GCS NIO                  | [spark-bam]  | 273      |
| GCS NIO with [pre-fetch] | [Disq]       | 152      |
| HDFS                     | [Disq]       | 167      |
| HDFS                     | [Hadoop-BAM] | 173      |

[Disq] does better than [Hadoop-BAM] using the GCS Connector since it
computes splits in parallel on the cluster, and it caches blocks of data to
permit efficient seeks (forwards and backwards in the stream). On HDFS the
difference is minimal (probably because HDFS itself does caching).

[Disq] is comparable to [spark-bam] when using the NIO filesystem connector
for GCS, but is better when [pre-fetch] is used. It may be possible to improve
the time further by tuning the size of the buffer (benchmarked at 4MB).

### Accuracy

[Hadoop-BAM] is known to produce both
[false negatives and false positives][spark-bam-accuracy] when checking if a
virtual offset in a BAM file is a record start, whereas [spark-bam] does
not produce any false readings.

Using the `DisqCheckBam` program on the source data, no false negatives or
false positives were recorded.

## Running

The _download.sh_ script is for retrieving the source data and storing in the
cloud. The paths will need changing if you want to store the data in a
different bucket.

The _run.sh_ script assembles the benchmarking code and runs the commands.
Some of the variables may need changing for your environment.

Timing results are written to _results/run.csv_, except for [spark-bam] which
writes to the console.

[Disq]: https://github.com/tomwhite/disq
[Hadoop-BAM]: https://github.com/HadoopGenomics/Hadoop-BAM
[spark-bam]: http://www.hammerlab.org/spark-bam/
[pre-fetch]: https://github.com/tomwhite/disq/tree/nio_prefetcher
[spark-bam-accuracy]: http://www.hammerlab.org/spark-bam/benchmarks#accuracy