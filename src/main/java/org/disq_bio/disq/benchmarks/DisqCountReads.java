package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.HtsjdkReadsRddStorage;

public class DisqCountReads {

  private static long countReads(String path, String filetype, String refPath, String sparkMaster, boolean useNio, int splitSize)
          throws IOException {
    try (JavaSparkContext jsc =
                 new JavaSparkContext(sparkMaster, DisqCountReads.class.getSimpleName())) {
      if (filetype.toUpperCase().equals("CRAM")) {

        return HtsjdkReadsRddStorage.makeDefault(jsc)
                .referenceSourcePath(refPath)
                .useNio(useNio)
                .splitSize(splitSize)
                .validationStringency(ValidationStringency.SILENT)
                .read(path)
                .getReads()
                .count();

      } else if (filetype.toUpperCase().equals("BAM")) {

        return HtsjdkReadsRddStorage.makeDefault(jsc)
                .useNio(useNio)
                .splitSize(splitSize)
                .validationStringency(ValidationStringency.SILENT)
                .read(path)
                .getReads()
                .count();

      } else {
        System.err.println("Unexpected filetype " + filetype);
        return 1L;
      }
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 6) {
      System.err.println("Usage: DisqCountReads <BAM/CRAM file> <filetype CRAM/BAM> <CRAM reference path> <spark master> <use NIO> <split size>");
      System.exit(1);
    }
    String path = args[0];
    String filetype = args[1];
    String refPath = args[2];
    String sparkMaster = args[3];
    boolean useNio = Boolean.parseBoolean(args[4]);
    int splitSize = Integer.parseInt(args[5]);
    long start = System.currentTimeMillis();
    System.out.println(countReads(path, filetype, refPath, sparkMaster, useNio, splitSize));
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
