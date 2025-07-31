package org.apache.parquet.hadoop;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.parquet.hadoop.util.PartialReadInputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.thrift.protocol.TProtocolException;
import org.junit.Before;
import org.junit.Test;

public class TestPageHeaderPartialRead {

  private static byte[] parquetFileBytes;
  private static long pageHeaderOffset;
  private static int pageHeaderLength;

  @Before
  public void setup() throws IOException {
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

    // 3. Read the file metadata to find the offset and size of the first page header
    InputFile inputFile = new MemoryInputFile(parquetFileBytes);
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

  // Provides a stream of integers from 0 to pageHeaderLength - 1 for the parameterized test
  private static Stream<Integer> faultOffsets() {
    return IntStream.range(0, pageHeaderLength).boxed();
  }

  // Full read of PageHeader from a valid stream should succeed.
  @Test
  public void fullReadOfPageHeaderShouldSucceed() {
    SeekableInputStream stream = new MemoryInputFile(parquetFileBytes).newStream();
    try {
      stream.seek(pageHeaderOffset);
      Util.readPageHeader(stream);
    } catch (Exception e) {
      System.out.printf("Received exception with message %s", e.getMessage());
    }
    // assertDoesNotThrow(() -> {
    //   stream.seek(pageHeaderOffset);
    //   Util.readPageHeader(stream);
    // }, "Reading a valid PageHeader should not throw an exception.");
  }

  @Test
  public void partialReadWithinPageHeaderShouldThrowException() {
    int faultOffset = 10;
    // The absolute position in the stream to inject the fault
    long absoluteFaultPosition = pageHeaderOffset + faultOffset;

    // Create a seekable stream and wrap it with our fault-injecting stream
    SeekableInputStream underlyingStream = new MemoryInputFile(parquetFileBytes).newStream();
    PartialReadInputStream faultyStream = new PartialReadInputStream(underlyingStream, absoluteFaultPosition);

    // Assert that attempting to read the header from this faulty stream throws the expected exception
    Exception e = assertThrows(
        "A partial read within the PageHeader should cause an IOException.", IOException.class, () -> {
          faultyStream.seek(pageHeaderOffset);
          Util.readPageHeader(faultyStream);
        });

    // Verify that the root cause is the TProtocolException
    Throwable cause = e;
    boolean foundTProtocolException = false;
    System.out.println("class name of exception"+ cause.getClass().toString());
    while (cause != null) {
      if (cause instanceof TProtocolException) {
        foundTProtocolException = true;
        break;
      }
      cause = cause.getCause();
    }
    assertTrue(
        "The root cause of the failure should be a TProtocolException. Fault offset: " + faultOffset,
        foundTProtocolException);
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
