package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.ValidationStringency;
import org.apache.hadoop.io.LongWritable;
import org.apache.spark.api.java.JavaSparkContext;
import org.seqdoop.hadoop_bam.AnySAMInputFormat;
import org.seqdoop.hadoop_bam.CRAMInputFormat;
import org.seqdoop.hadoop_bam.SAMRecordWritable;
import org.seqdoop.hadoop_bam.util.SAMHeaderReader;

public class HadoopBamCountReads {

  private static long countReads(String path, String filetype, String refPath, String sparkMaster) {
    try (JavaSparkContext jsc =
                 new JavaSparkContext(sparkMaster, HadoopBamCountReads.class.getSimpleName())) {
      if (filetype.toUpperCase().equals("BAM")) {

        jsc.hadoopConfiguration().set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.name());

        return jsc.newAPIHadoopFile(
                path,
                AnySAMInputFormat.class,
                LongWritable.class,
                SAMRecordWritable.class,
                jsc.hadoopConfiguration())
                .count();

      } else if (filetype.toUpperCase().equals("CRAM")) {

        jsc.hadoopConfiguration().set(CRAMInputFormat.REFERENCE_SOURCE_PATH_PROPERTY, refPath);
        jsc.hadoopConfiguration().set(SAMHeaderReader.VALIDATION_STRINGENCY_PROPERTY, ValidationStringency.SILENT.name());

        return jsc.newAPIHadoopFile(
                path,
                AnySAMInputFormat.class,
                LongWritable.class,
                SAMRecordWritable.class,
                jsc.hadoopConfiguration())
                .count();

      } else {
        System.err.println("Unexpected filetype " + filetype);
        return 1L;
      }

    }
  }

  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Usage: HadoopBamCountReads <CRAM/BAM file> <filetype CRAM/BAM> <CRAM reference path> <spark master>");
      System.exit(1);
    }
    String path = args[0];
    String filetype = args[1];
    String refPath = args[2];
    String sparkMaster = args[3];
    long start = System.currentTimeMillis();
    System.out.println(countReads(path, filetype, refPath, sparkMaster));
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
