package org.disq_bio.disq.benchmarks;

import java.io.IOException;
import java.util.Map;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.impl.formats.bam.BamRecordGuesserChecker;

public class DisqCheckBam {
  private static Map<Long, BamRecordGuesserChecker.RecordStartResult> check(
      String path, String sparkMaster, boolean useNio, int splitSize) throws IOException {
    try (JavaSparkContext jsc =
        new JavaSparkContext(sparkMaster, DisqCheckBam.class.getSimpleName())) {
      BamRecordGuesserChecker bamRecordGuesserChecker = new BamRecordGuesserChecker(useNio);
      return bamRecordGuesserChecker.check(jsc, path, splitSize).collectAsMap();
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 4) {
      System.err.println("Usage: DisqCheckBam <BAM file> <spark master> <use NIO> <split size>");
      System.exit(1);
    }
    String path = args[0];
    String sparkMaster = args[1];
    boolean useNio = Boolean.valueOf(args[2]);
    int splitSize = Integer.valueOf(args[3]);
    long start = System.currentTimeMillis();
    Map<Long, BamRecordGuesserChecker.RecordStartResult> check =
        check(path, sparkMaster, useNio, splitSize);
    if (check.isEmpty()) {
      System.out.println("No mismatches found");
    } else {
      check.forEach((pos, result) -> System.out.printf("%s\t%s\n", pos, result));
    }
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
