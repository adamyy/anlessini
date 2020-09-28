package io.anlessini.store;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.BufferedIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

public class S3IndexInput extends BufferedIndexInput {
  private static final Logger LOG = LogManager.getLogger(S3IndexInput.class);
  private static final int CHUNK_SIZE = 1024 * 1024 * 64; // 64 MB

  private final AmazonS3 s3Client;
  private final S3ObjectSummary objectSummary;
  private ByteBuffer bytebuf;

  /**
   * The start offset in the entire file, non-zero in the slice case
   */
  private final long off;
  /**
   * The end offset
   */
  private final long end;

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary objectSummary) {
    this(s3Client, objectSummary, 0, objectSummary.getSize(), defaultBufferSize(objectSummary.getSize()));
  }

  public S3IndexInput(AmazonS3 s3Client, S3ObjectSummary objectSummary, long offset, long length, int bufferSize) {
    super(objectSummary.getBucketName() + "/" + objectSummary.getKey(), bufferSize);
    this.s3Client = s3Client;
    this.objectSummary = objectSummary;
    this.off = offset;
    this.end = offset + length;
    LOG.info("Opened S3IndexInput " + toString() + " , bufferSize=" + getBufferSize() + ", this=" + hashCode());
  }

  private static int defaultBufferSize(long fileLength) {
    long bufferSize = fileLength;
    bufferSize = Math.max(bufferSize, MIN_BUFFER_SIZE);
    bufferSize = Math.min(bufferSize, CHUNK_SIZE);
    return Math.toIntExact(bufferSize);
  }

  @Override
  public void close() throws IOException {
    // no-op
  }

  @Override
  public long length() {
    return end - off;
  }

  @Override
  protected void newBuffer(byte[] newBuffer) {
    super.newBuffer(newBuffer);
    bytebuf = ByteBuffer.wrap(newBuffer);
  }

  @Override
  public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
    if (offset < 0 || length < 0 || offset + length > this.length()) {
      throw new IllegalArgumentException("Slice " + sliceDescription + " out of bounds: " +
          "offset=" + offset + ",length=" + length + ",fileLength="  + this.length() + ": "  + toString());
    }
    return new S3IndexInput(s3Client, objectSummary, off + offset, length, defaultBufferSize(length));
  }

  @Override
  protected void readInternal(byte[] b, int offset, int length) throws IOException {
    final ByteBuffer bb;

    if (b == buffer) { // using internal
      assert bytebuf != null;
      bytebuf.clear().position(offset);
      bb = bytebuf;
    } else {
      bb = ByteBuffer.wrap(b, offset, length);
    }

    synchronized (this) {
      long pos = getFilePointer() + this.off;

      if (pos + length > end) {
        throw new EOFException("Reading past EOF: " + toString() + "@" + hashCode());
      }

      try {
        int readLength = length;
        while (readLength > 0) {
          final int toRead = Math.min(CHUNK_SIZE, readLength);
          bb.limit(bb.position() + toRead);
          assert bb.remaining() == toRead;
          final int bytesRead = readFromS3(bb, pos, toRead);
          if (bytesRead < 0) {
            throw new EOFException("Read past EOF: " + toString() + "@" + hashCode() + " off=" + offset + " len=" + length + " pos=" + pos + " chunk=" + toRead + " end=" + end);
          }
          assert bytesRead > 0 : "Read with non zero-length bb.remaining() must always read at least one byte";
          pos += bytesRead;
          readLength -= bytesRead;
        }
        assert readLength == 0;
      } catch (IOException ioe) {
        throw new IOException(ioe.getMessage() + ": " + toString() + "@" + hashCode(), ioe);
      }
    }
  }

  protected int readFromS3(ByteBuffer b, long offset, int length) throws IOException {
    LOG.info("[readingFromS3][" + toString() + "] offset=" + offset + " length=" + length + ", this=" + hashCode());
    GetObjectRequest rangeObjectRequest = new GetObjectRequest(objectSummary.getBucketName(), objectSummary.getKey())
        .withRange(offset, offset + length - 1);
    S3Object object = s3Client.getObject(rangeObjectRequest);
    ReadableByteChannel objectContentChannel = Channels.newChannel(object.getObjectContent());
    int bytesRead = IOUtils.read(objectContentChannel, b);
    object.close();
    return bytesRead;
  }

  @Override
  protected void seekInternal(long pos) throws IOException {
    if (pos > length()) {
      throw new EOFException("read past EOF: pos=" + pos + ", length=" + length() + ": " + toString() + "@" + hashCode());
    }
  }
}