package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.util.Interval;
import java.io.IOException;
import java.util.Collections;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.HtsjdkReadsRddStorage;
import org.disq_bio.disq.HtsjdkReadsTraversalParameters;

public class DisqCountReads {

  private static long countReads(String path, String sparkMaster, boolean useNio, int splitSize)
      throws IOException {
    try (JavaSparkContext jsc =
        new JavaSparkContext(sparkMaster, DisqCountReads.class.getSimpleName())) {
      Interval chr1 = new Interval("chr1", 1, Integer.MAX_VALUE);
      HtsjdkReadsTraversalParameters<Interval> traversalParameters =
          new HtsjdkReadsTraversalParameters<>(Collections.singletonList(chr1), false);
      return HtsjdkReadsRddStorage.makeDefault(jsc)
          .useNio(useNio)
          .splitSize(splitSize)
          .validationStringency(ValidationStringency.SILENT)
          .read(path, traversalParameters)
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
