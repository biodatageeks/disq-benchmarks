#!/bin/bash -x

DISQ_BENCH_JAR=disq-benchmarks-0.0.2-SNAPSHOT.jar
gsutil cp gs://bdg-sequila-dev/benchmark/jars/$DISQ_BENCH_JAR /tmp

DISQ_BENCH_JAR_PATH=/tmp/$DISQ_BENCH_JAR

count-reads() {
    CLASS=$1
    ARGS=$2
    LOG=logs/${CLASS}_$(date +%Y%m%d_%H%M%S).log
    RESULTS_CSV=results/run.csv
    spark-submit -v \
    --master $(python get-spark-arg.py --master) \
    $(python get-spark-arg.py --conf) \
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
MID_SIZED_BAM=gs://bdg-sequila-dev/data/b1_0_uncompressed.bam
count-reads org.disq_bio.disq.benchmarks.DisqCountReads "$MID_SIZED_BAM \
      $(python get-spark-arg.py --master) \
      true \
      134217728"