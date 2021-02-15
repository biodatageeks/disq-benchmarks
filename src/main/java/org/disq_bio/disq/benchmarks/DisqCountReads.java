package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.ValidationStringency;
import java.io.IOException;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.HtsjdkReadsRddStorage;

public class DisqCountReads {

  private static long countReads(String path,
                                 String filetype,
                                 String refPath,
                                 String sparkMaster,
                                 boolean useNio,
                                 int splitSize,
                                 boolean kryo)
          throws IOException {
    try (JavaSparkContext jsc =
                 new JavaSparkContext(sparkMaster, DisqCountReads.class.getSimpleName())) {
      if (filetype.toUpperCase().equals("CRAM")) {

        if (kryo) {
          jsc.getConf()
                  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  .set("spark.kryo.registrator", "org.disq_bio.disq.serializer.DisqKryoRegistrator")
                  .set("spark.kryo.referenceTracking", "true");
        }

        return HtsjdkReadsRddStorage.makeDefault(jsc)
                .referenceSourcePath(refPath)
                .useNio(useNio)
                .splitSize(splitSize)
                .validationStringency(ValidationStringency.SILENT)
                .read(path)
                .getReads()
                .count();

      } else if (filetype.toUpperCase().equals("BAM")) {

        if (kryo) {
          jsc.getConf()
                  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                  .set("spark.kryo.registrator", "org.disq_bio.disq.serializer.DisqKryoRegistrator")
                  .set("spark.kryo.referenceTracking", "true");
        }

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
    if (args.length != 7) {
      System.err.println("Usage: DisqCountReads <BAM/CRAM file> <filetype CRAM/BAM> <CRAM reference path> <spark master> <use NIO> <split size> <use kryo>");
      System.exit(1);
    }
    String path = args[0];
    String filetype = args[1];
    String refPath = args[2];
    String sparkMaster = args[3];
    boolean useNio = Boolean.parseBoolean(args[4]);
    int splitSize = Integer.parseInt(args[5]);
    boolean kryo = Boolean.parseBoolean(args[6]);
    long start = System.currentTimeMillis();
    System.out.println(countReads(path, filetype, refPath, sparkMaster, useNio, splitSize, kryo));
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
