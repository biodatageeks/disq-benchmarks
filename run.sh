# Env variables
export DATAPROC_CLUSTER_NAME=tw-cluster2
export CLOUDSDK_CORE_PROJECT=broad-gatk-collab
export CLI_JAR=spark-bam-cli.jar
export GOOGLE_CLOUD_NIO_JAR=google-cloud-nio-0.20.0-alpha-shaded.jar

# Run a dataproc cluster with 5 worker nodes, 200GB of disk, 8 vcores
gcloud dataproc clusters create $DATAPROC_CLUSTER_NAME --subnet default --zone us-central1-b --master-machine-type n1-standard-8 --master-boot-disk-size 500 --num-workers 5 --worker-machine-type n1-standard-8 --worker-boot-disk-size 2000 --image-version 1.2 --project broad-gatk-collab

# Build the benchmark code and store in a bucket
mvn clean install
gsutil cp target/disq-benchmarks-0.0.1-SNAPSHOT.jar gs://disq-tom-testdata/jars/disq-benchmarks-0.0.1-SNAPSHOT.jar

# spark-bam
wget -O $CLI_JAR https://oss.sonatype.org/content/repositories/releases/org/hammerlab/bam/cli_2.11/1.2.0-M1/cli_2.11-1.2.0-M1-assembly.jar
gsutil cp $CLI_JAR gs://disq-tom-testdata/jars/$CLI_JAR
rm $CLI_JAR

wget https://oss.sonatype.org/content/repositories/releases/com/google/cloud/google-cloud-nio/0.20.0-alpha/$GOOGLE_CLOUD_NIO_JAR
gsutil cp $GOOGLE_CLOUD_NIO_JAR gs://disq-tom-testdata/jars/$GOOGLE_CLOUD_NIO_JAR
rm $GOOGLE_CLOUD_NIO_JAR

count-reads() {
    CLASS=$1
    ARGS=$2
    LOG=logs/${CLASS}_$(date +%Y%m%d_%H%M%S).log
    RESULTS_CSV=results/run.csv
    gcloud dataproc jobs submit spark --cluster $DATAPROC_CLUSTER_NAME \
     --project $CLOUDSDK_CORE_PROJECT \
     --properties spark.executor.instances=5,spark.executor.cores=8,spark.executor.memory=4g,spark.driver.memory=4g,spark.dynamicAllocation.enabled=false \
     --jars gs://disq-tom-testdata/jars/disq-benchmarks-0.0.1-SNAPSHOT.jar \
     --class $CLASS \
     -- \
     $ARGS 2>&1 | tee /dev/tty > $LOG
    RC=$?
    DURATION_SEC=$(grep 'Time taken' $LOG | grep -Eo "[0-9]+")
    echo "$CLASS,$ARGS,$RC,$DURATION_SEC" >> $RESULTS_CSV
}

count-reads-sparkbam() {
    ARGS=$1
    gcloud dataproc jobs submit spark --cluster $DATAPROC_CLUSTER_NAME \
     --project $CLOUDSDK_CORE_PROJECT \
     --properties spark.executor.instances=5,spark.executor.cores=8,spark.executor.memory=4g,spark.driver.memory=4g,spark.dynamicAllocation.enabled=false \
     --jars gs://disq-tom-testdata/jars/$GOOGLE_CLOUD_NIO_JAR \
     --jar gs://disq-tom-testdata/jars/$CLI_JAR \
     -- \
     count-reads $ARGS
}

MID_SIZED_BAM=gs://disq-tom-testdata/1000genomes/ftp/data/HG00096/alignment/HG00096.mapped.ILLUMINA.bwa.GBR.low_coverage.20120522.bam

count-reads com.tom_e_white.disq.benchmarks.DisqCountReads "$MID_SIZED_BAM yarn false 134217728"
count-reads com.tom_e_white.disq.benchmarks.DisqCountReads "$MID_SIZED_BAM yarn true 134217728"
count-reads com.tom_e_white.disq.benchmarks.HadoopBamCountReads "$MID_SIZED_BAM yarn"

LARGE_SIZED_BAM=gs://disq-tom-testdata/giab/ftp/data/NA12878/NA12878_PacBio_MtSinai/sorted_final_merged.bam

count-reads com.tom_e_white.disq.benchmarks.DisqCountReads "$LARGE_SIZED_BAM yarn false 134217728"
count-reads com.tom_e_white.disq.benchmarks.DisqCountReads "$LARGE_SIZED_BAM yarn true 134217728"
count-reads com.tom_e_white.disq.benchmarks.HadoopBamCountReads "$LARGE_SIZED_BAM yarn"
count-reads-sparkbam $LARGE_SIZED_BAM

