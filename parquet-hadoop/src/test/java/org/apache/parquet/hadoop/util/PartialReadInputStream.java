package org.apache.parquet.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

/**
 * A SeekableInputStream wrapper that simulates a partial read at a specific position.
 * It reads normally until a designated fault position is reached. At that point,
 * return -1 to simulate EOF or return a single byte to signify a partial read.
 */
public class PartialReadInputStream extends DelegatingSeekableInputStream {

  private final long faultTriggerPosition;
  private boolean faultInjected = false;
  private boolean shouldFailWithEOF = true;

  /**
   * Constructs a PartialReadInputStream.
   *
   * @param stream The underlying InputStream to wrap.
   * @param faultTriggerPosition The absolute position in the stream at which to inject a partial read.
   */
  public PartialReadInputStream(InputStream stream, long faultTriggerPosition, boolean shouldFailWithEOF) {
    super(stream);
    this.faultTriggerPosition = faultTriggerPosition;
    this.shouldFailWithEOF = shouldFailWithEOF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (shouldFailWithEOF) {
      return readInternalEOF(b, off, len);
    }
    return readInternalPartial(b, off, len);
  }

  private int readInternalPartial(byte[] b, int off, int len) throws IOException {
    long currentPos = getPos();

    if (!faultInjected && currentPos >= faultTriggerPosition) {
      // Inject the fault: perform a single-byte read to simulate a partial read.
      faultInjected = true;
      int byteRead = super.read();
      if (byteRead == -1) {
        return -1;
      }
      b[off] = (byte) byteRead;
      return 1; // Return 1 to signify a partial read.
    }
    return super.read(b, off, len);
  }

  private int readInternalEOF(byte[] b, int off, int len) throws IOException {
    long currentPos = getPos();

    // If the read position is at or beyond the fault trigger,
    // simulate a premature end-of-file.
    if (currentPos >= faultTriggerPosition) {
      faultInjected = true;
      return -1; // Simulate EOF
    }

    // If the current read request would cross the fault boundary,
    // truncate the read to stop exactly at the fault position.
    if ((currentPos + len) > faultTriggerPosition) {
      int truncatedLen = (int) (faultTriggerPosition - currentPos);
      return super.read(b, off, truncatedLen);
    }

    // Otherwise, perform a normal read.
    return super.read(b, off, len);
  }

  // The following methods must be implemented as DelegatingSeekableInputStream is abstract.
  // They should delegate to the underlying stream if it is seekable, or throw if not.
  // For this test's purpose (using ByteArrayInputStream), these are sufficient.

  @Override
  public long getPos() throws IOException {
    // This assumes the underlying stream is a ByteArrayInputStream or similar
    // where available() can be used to infer position. A more robust implementation
    // would require a truly seekable stream.
    return ((DelegatingSeekableInputStream) getStream()).getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    ((DelegatingSeekableInputStream) getStream()).seek(newPos);
  }
}
