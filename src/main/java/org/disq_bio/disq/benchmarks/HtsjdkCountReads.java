package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.SAMRecord;
import htsjdk.samtools.SamInputResource;
import htsjdk.samtools.SamReader;
import htsjdk.samtools.SamReaderFactory;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;

public class HtsjdkCountReads {

  private static long countReads(String path) throws IOException {
    long recCount = 0;
    try (SamReader samReader =
        SamReaderFactory.makeDefault().open(SamInputResource.of(Paths.get(URI.create(path))))) {
      for (SAMRecord record : samReader) {
        recCount++;
      }
    }
    return recCount;
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 1) {
      System.err.println("Usage: HtsjdkCountReads <BAM file>");
      System.exit(1);
    }
    String path = args[0];
    long start = System.currentTimeMillis();
    System.out.println(countReads(path));
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
