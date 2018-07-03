package com.tom_e_white.disq.benchmarks;

import com.tom_e_white.disq.HtsjdkReadsRddStorage;
import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;

public class DisqCountReads {

  private static long countReads(String path, String sparkMaster, boolean useNio, int splitSize)
      throws IOException {
    try (JavaSparkContext jsc =
        new JavaSparkContext(sparkMaster, DisqCountReads.class.getSimpleName())) {
      return HtsjdkReadsRddStorage.makeDefault(jsc)
          .useNio(useNio)
          .splitSize(splitSize)
          .validationStringency(ValidationStringency.SILENT)
          .read(path)
          .getReads()
          .count();
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 4) {
      System.err.println("Usage: DisqCountReads <BAM file> <spark master> <use NIO> <split size>");
      System.exit(1);
    }
    String path = args[0];
    String sparkMaster = args[1];
    boolean useNio = Boolean.valueOf(args[2]);
    int splitSize = Integer.valueOf(args[3]);
    long start = System.currentTimeMillis();
    System.out.println(countReads(path, sparkMaster, useNio, splitSize));
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
