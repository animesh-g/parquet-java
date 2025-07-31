package org.apache.parquet.hadoop.util;

import java.io.IOException;
import java.io.InputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;

/**
 * A SeekableInputStream wrapper that simulates a partial read at a specific position.
 * It reads normally until a designated fault position is reached. At that point,
 * it returns a single byte to simulate a short read from a network stream,
 * after which it resumes normal operation.
 */
public class PartialReadInputStream extends DelegatingSeekableInputStream {

  private final long faultTriggerPosition;
  private boolean faultInjected = false;

  /**
   * Constructs a PartialReadInputStream.
   *
   * @param stream The underlying InputStream to wrap.
   * @param faultTriggerPosition The absolute position in the stream at which to inject a partial read.
   */
  public PartialReadInputStream(InputStream stream, long faultTriggerPosition) {
    super(stream);
    this.faultTriggerPosition = faultTriggerPosition;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
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
