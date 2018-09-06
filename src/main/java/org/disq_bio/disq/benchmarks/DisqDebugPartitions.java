package org.disq_bio.disq.benchmarks;

import htsjdk.samtools.BAMSBIIndexer;
import htsjdk.samtools.SBIIndex;
import htsjdk.samtools.ValidationStringency;
import htsjdk.samtools.seekablestream.SeekableStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.disq_bio.disq.impl.file.FileSystemWrapper;
import org.disq_bio.disq.impl.file.HadoopFileSystemWrapper;
import org.disq_bio.disq.impl.file.NioFileSystemWrapper;
import org.disq_bio.disq.impl.file.PathChunk;
import org.disq_bio.disq.impl.formats.bam.BamSource;

public class DisqDebugPartitions {

  private final FileSystemWrapper fileSystemWrapper;

  public DisqDebugPartitions(boolean useNio) {
    this.fileSystemWrapper = useNio ? new NioFileSystemWrapper() : new HadoopFileSystemWrapper();
  }

  private static void debug(String path, String sparkMaster, boolean useNio, int splitSize)
      throws IOException {
    try (JavaSparkContext jsc =
        new JavaSparkContext(sparkMaster, DisqDebugPartitions.class.getSimpleName())) {
      DisqDebugPartitions disqDebugPartitions = new DisqDebugPartitions(useNio);

      JavaRDD<PathChunk> pathChunks = disqDebugPartitions.getPathChunks(jsc, path, splitSize);
      System.out.println("Path chunks");
      for (PathChunk pathChunk : pathChunks.collect()) {
        System.out.println(pathChunk);
      }
      // SBIIndex sbiIndex = disqDebugPartitions.getSBIIndex(jsc.hadoopConfiguration(), path);
    }
  }

  static class ExtendedBamSource extends BamSource {
    public ExtendedBamSource(FileSystemWrapper fileSystemWrapper) {
      super(fileSystemWrapper);
    }

    @Override
    protected JavaRDD<PathChunk> getPathChunks(
        JavaSparkContext jsc,
        String path,
        int splitSize,
        ValidationStringency stringency,
        String referenceSourcePath)
        throws IOException {
      return super.getPathChunks(jsc, path, splitSize, stringency, referenceSourcePath);
    }
  }

  JavaRDD<PathChunk> getPathChunks(JavaSparkContext jsc, String path, int splitSize)
      throws IOException {
    ExtendedBamSource bamSource = new ExtendedBamSource(fileSystemWrapper);
    return bamSource.getPathChunks(jsc, path, splitSize, ValidationStringency.SILENT, null);
  }

  SBIIndex getSBIIndex(Configuration conf, String bamFile) throws IOException {
    String sbiFile = bamFile + SBIIndex.FILE_EXTENSION;
    if (!fileSystemWrapper.exists(conf, sbiFile)) {
      // create SBI file
      try (SeekableStream in = fileSystemWrapper.open(conf, bamFile);
          OutputStream out = fileSystemWrapper.create(conf, sbiFile)) {
        BAMSBIIndexer.createIndex(in, out, 1);
      }
    }
    try (SeekableStream in = fileSystemWrapper.open(conf, sbiFile)) {
      return SBIIndex.load(in);
    }
  }

  public static void main(String[] args) throws IOException {
    if (args.length != 4) {
      System.err.println(
          "Usage: DisqDebugPartitions <BAM file> <spark master> <use NIO> <split size>");
      System.exit(1);
    }
    String path = args[0];
    String sparkMaster = args[1];
    boolean useNio = Boolean.valueOf(args[2]);
    int splitSize = Integer.valueOf(args[3]);
    long start = System.currentTimeMillis();
    debug(path, sparkMaster, useNio, splitSize);
    long end = System.currentTimeMillis();
    System.out.printf("Time taken: %ss\n", (end - start) / 1000);
  }
}
