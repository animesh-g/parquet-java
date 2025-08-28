package org.apache.parquet.hadoop;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.PartialReadInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestPageHeaderPartialRead {

  // Flag to control read source.
  // true: read from a pre-existing file on disk.
  // false: create a file in memory and read from it.
  private static final boolean READ_FROM_DISK = true;

  // Path to the pre-existing Parquet file.
  // This file should be placed in the /dev/shm directory to insure in-memory reads.
  private static final String PARQUET_FILE_PATH = "gs://anim_test-bucket-1/titanic.parquet";

  // Offset at which fault will occur.
  private static final int FAULT_OFFSET = 10;

  private static Configuration conf = new Configuration();
  private static Path filePath;

  private static byte[] parquetFileBytes;
  private static long pageHeaderOffset;
  private static int pageHeaderLength;

  @Before
  public void setup() throws IOException {
    if (READ_FROM_DISK) {
      File file = new File(PARQUET_FILE_PATH);
      filePath = new Path(file.toURI());
    } else {
      // 1. Define a simple schema
      MessageType schema = Types.buildMessage()
          .required(PrimitiveTypeName.BINARY)
          .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
          .named("name")
          .named("test_schema");

      Configuration conf = new Configuration();
      GroupWriteSupport.setSchema(schema, conf);
      SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

      // 2. Write a simple Parquet file to an in-memory byte array
      ByteArrayOutputStream baos = new ByteArrayOutputStream();

      GroupWriteSupport.setSchema(schema, conf);
      OutputFile newFile = new MemoryOutputFile(baos);

      try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(newFile)
          .withConf(conf)
          .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
          .withRowGroupSize(1024)
          .withPageSize(1024)
          .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
          .build()) {
        writer.write(groupFactory.newGroup().append("name", "parquet"));
      }
      parquetFileBytes = baos.toByteArray();
    }

    // 3. Read the file metadata to find the offset and size of the first page header
    InputFile inputFile = getInputFile();
    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
      ParquetMetadata footer = reader.getFooter();
      assertTrue("No row groups found in test file", footer.getBlocks().size() > 0);
      BlockMetaData block = footer.getBlocks().get(0);
      assertTrue("No column chunks found in test file", block.getColumns().size() > 0);

      pageHeaderOffset = block.getColumns().get(0).getFirstDataPageOffset();

      // To get the header length, we must read it from a valid stream
      try (SeekableInputStream stream = inputFile.newStream()) {
        stream.seek(pageHeaderOffset);
        PageHeader header = Util.readPageHeader(stream);
        long endOfHeaderPos = stream.getPos();
        pageHeaderLength = (int) (endOfHeaderPos - pageHeaderOffset);
      }
    }
    assertTrue("Could not determine page header length", pageHeaderLength > 0);
  }

  private static InputFile getInputFile() throws IOException {
    if (PARQUET_FILE_PATH.startsWith("gs://")) {
      // Set properties to enable gRPC and replicate the production environment.
      conf.setBoolean("fs.gs.grpc.enable", true);
      conf.set("fs.gs.client.type", "STORAGE_CLIENT");
      // conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
      // conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
      conf.set("fs.gs.inputstream.fadvise", "AUTO_RANDOM");
      conf.setInt("fs.gs.inputstream.min.range.request.size", 1048576);
      conf.setBoolean("fs.gs.inputstream.fast.fail.on.not.found.enable", true);
      conf.setBoolean("fs.gs.client.caching.experiment.enabled", true);
      conf.setBoolean("fs.gs.stream.caller.context.enable", true);
    }
    if (READ_FROM_DISK) {
      return HadoopInputFile.fromPath(filePath, conf);
    } else {
      return new MemoryInputFile(parquetFileBytes);
    }
  }

  // Full read of PageHeader from a valid stream should succeed.
  @Test
  public void fullReadOfPageHeaderShouldSucceed() {
    try (SeekableInputStream stream = getInputFile().newStream()) {
      stream.seek(pageHeaderOffset);
      Util.readPageHeader(stream);
    } catch (Exception e) {
      Assert.fail("Received exception with message " + e.getMessage());
    }
  }

  @Test
  public void intermediatePartialReadWithinPageHeaderShouldNotThrowException() throws IOException {
    // The absolute position in the stream to inject the fault
    long absoluteFaultPosition = pageHeaderOffset + FAULT_OFFSET;

    try (SeekableInputStream underlyingStream = getInputFile().newStream()) {
      // Create a seekable stream and wrap it with our fault-injecting stream
      PartialReadInputStream faultyStream =
          new PartialReadInputStream(underlyingStream, absoluteFaultPosition, false);
      try {
        faultyStream.seek(pageHeaderOffset);
        Util.readPageHeader(faultyStream);
      } catch (Exception e) {
        Assert.fail("Received exception with message " + e.getMessage());
      }
    }
  }

  @Test
  public void partialReadWithinPageHeaderShouldThrowException() throws IOException {
    // The absolute position in the stream to inject the fault
    long absoluteFaultPosition = pageHeaderOffset + FAULT_OFFSET;

    // Create a seekable stream and wrap it with our fault-injecting stream
    try (SeekableInputStream underlyingStream = getInputFile().newStream()) {
      PartialReadInputStream faultyStream =
          new PartialReadInputStream(underlyingStream, absoluteFaultPosition, true);

      // Assert that attempting to read the header from this faulty stream throws the expected exception
      assertThrows("A partial read within the PageHeader should cause an IOException.", IOException.class, () -> {
        faultyStream.seek(pageHeaderOffset);
        Util.readPageHeader(faultyStream);
      });
    }
  }

  // Helper classes for in-memory file handling
  private static class MemoryInputFile implements InputFile {
    private final byte[] data;

    public MemoryInputFile(byte[] data) {
      this.data = data;
    }

    @Override
    public long getLength() {
      return this.data.length;
    }

    @Override
    public SeekableInputStream newStream() {
      return new SeekableByteArrayInputStream(data);
    }
  }

  /**
   * A simple implementation of OutputFile that writes to a ByteArrayOutputStream.
   */
  private static class MemoryOutputFile implements OutputFile {
    private final ByteArrayOutputStream baos;

    public MemoryOutputFile(ByteArrayOutputStream baos) {
      this.baos = baos;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
      return new PositionOutputStream() {
        private long position = 0;

        @Override
        public long getPos() {
          return position;
        }

        @Override
        public void write(int b) throws IOException {
          baos.write(b);
          position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
          baos.write(b, off, len);
          position += len;
        }
      };
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
      // For this in-memory example, create and createOrOverwrite are the same.
      baos.reset();
      return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  private static class SeekableByteArrayInputStream extends DelegatingSeekableInputStream {
    private final ByteArrayInputStream stream;

    public SeekableByteArrayInputStream(byte[] bytes) {
      super(new ByteArrayInputStream(bytes));
      this.stream = (ByteArrayInputStream) getStream();
    }

    @Override
    public long getPos() {
      return stream.available() > 0 ? parquetFileBytes.length - stream.available() : parquetFileBytes.length;
    }

    @Override
    public void seek(long newPos) {
      stream.reset();
      stream.skip(newPos);
    }
  }
}
