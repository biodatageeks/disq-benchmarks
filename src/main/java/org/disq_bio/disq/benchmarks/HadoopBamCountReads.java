package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.ValidationStringency;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

public class HadoopBamCountReads {

  private static long countReads(String path, String sparkMaster) {
    try (JavaSparkContext jsc =
        new JavaSparkContext(sparkMaster, HadoopBamCountReads.class.getSimpleName())) {
      jsc.hadoopConfiguration()
          .set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.name());
      return jsc.newAPIHadoopFile(
              path,
              AnySAMInputFormat.class,
              LongWritable.class,
              SAMRecordWritable.class,
              jsc.hadoopConfiguration())
          .count();
    }
  }

  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Usage: HadoopBamCountReads <BAM file> <spark master>");
      System.exit(1);
    }
    String path = args[0];
    String sparkMaster = args[1];
    long start = System.currentTimeMillis();
    System.out.println(countReads(path, sparkMaster));
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
