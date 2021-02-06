#!/bin/bash -x

EXECUTORS=1
FILETYPE=CRAM
CLASS_NAME=org.disq_bio.disq.benchmarks.DisqCountReads

REF_PATH=gs://bdg-sequila-dev/data/GRCh38_full_analysis_set_plus_decoy_hla.fa
CRAM_FILE=gs://bdg-sequila-dev/data/HG01879_chr1.cram
MID_SIZED_BAM=gs://bdg-sequila-dev/data/b1_0_uncompressed.bam

LOG_DIR=~/benchmark_logs
RESULTS_DIR=~/results

DISQ_BENCH_JAR=disq-benchmarks-0.0.2-SNAPSHOT.jar

gsutil cp gs://bdg-sequila-dev/benchmark/jars/$DISQ_BENCH_JAR /tmp

DISQ_BENCH_JAR_PATH=/tmp/$DISQ_BENCH_JAR

count-reads() {
    CLASS=$1
    ARGS=$2
    LOG=$LOG_DIR/${CLASS}_$(date +%Y%m%d_%H%M%S)_executors$EXECUTORS.log
    RESULTS_CSV=$RESULTS_DIR/run.csv
    spark-submit -v \
    --master $(python get-spark-arg.py --master) \
    $(python get-spark-arg.py --conf) \
    --conf spark.executor.instances=$EXECUTORS \
    --repositories $(python get-spark-arg.py --repositories) \
    --packages $(python get-spark-arg.py --packages) \
    --jars $(python get-spark-arg.py --jars) \
    --class $CLASS \
    $DISQ_BENCH_JAR_PATH \
     $ARGS 2>&1 | tee /dev/tty > $LOG
    RC=$?
    DURATION_SEC=$(grep 'Time taken' $LOG | grep -Eo "[0-9]+")
    echo "$CLASS,$ARGS,$RC,$DURATION_SEC" >> $RESULTS_CSV
}

count-reads "$CLASS_NAME" "$CRAM_FILE \
      $FILETYPE \
      $REF_PATH \
      $(python get-spark-arg.py --master) \
      true \
      134217728"