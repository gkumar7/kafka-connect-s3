package com.deviantart.kafka_connect_s3;

/**
 * Created by Geet Kumar geet.kumar@here.com on 8/19/16.
 */

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A wrapper for a simple output stream to keep track of the number of bytes written
 */
class CountingOutputStream extends FilterOutputStream {
	private long numBytes = 0;

	CountingOutputStream(OutputStream out) throws IOException {
		super(out);
	}

	@Override
	public void write(int b) throws IOException {
		out.write(b);
		numBytes++;
	}
	@Override
	public void write(byte[] b) throws IOException {
		out.write(b);
		numBytes += b.length;
	}
	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		out.write(b, off, len);
		numBytes += len;
	}

	public long getNumBytesWritten() {
		return numBytes;
	}
};