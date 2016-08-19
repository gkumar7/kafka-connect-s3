package com.deviantart.kafka_connect_s3;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.json.simple.JSONObject;

/**
 * BlockGZIPFileWriter accumulates newline delimited UTF-8 records and writes them to an
 * output file that is readable by GZIP.
 *
 * In fact this file is the concatenation of possibly many separate GZIP files corresponding to smaller chunks
 * of the input. Alongside the output filename.gz file, a file filename-index.json is written containing JSON
 * metadata about the size and location of each block.
 *
 * This allows a reading class to skip to particular line/record without decompressing whole file by looking up
 * the offset of the containing block, seeking to it and beginning GZIp read from there.
 *
 * This is especially useful when the file is an archive in HTTP storage like Amazon S3 where GET request with
 * range headers can allow pulling a small segment from overall compressed file.
 *
 * Note that thanks to GZIP spec, the overall file is perfectly valid and will compress as if it was a single stream
 * with any regular GZIP decoding library or program.
 */
public class JsonFileWriter {
  private String filenameBase;
  private String path;
  private BufferedWriter writer;
  private CountingOutputStream fileStream;

  private class JsonRecord {
    public long rawBytes = 0;
    public long recordOffset = 0;
  };

  private JsonRecord jr;
  private long recordOffset;

  public JsonFileWriter(String filenameBase, String path, long recordOffset)
  throws IOException
  {
    this.filenameBase = filenameBase;
    this.path = path;
    this.recordOffset = recordOffset;

    // Explicitly truncate the file. On linux and OS X this appears to happen
    // anyway when opening with FileOutputStream but that behavior is not actually documented
    // or specified anywhere so let's be rigorous about it.
    FileOutputStream fos = new FileOutputStream(new File(getDataFilePath()));
    fos.getChannel().truncate(0);

    // Open file for writing and setup
    this.fileStream = new CountingOutputStream(fos);
    this.writer = new BufferedWriter(new OutputStreamWriter(fileStream));
  }

  public String getDataFileName() {
    return String.format("%s-%012d.json", filenameBase, recordOffset);
  }

  public String getIndexFileName() {
    return String.format("%s-%012d.index.json", filenameBase, recordOffset);
  }

  public String getDataFilePath() {
    return String.format("%s/%s", path, this.getDataFileName());
  }

  public String getIndexFilePath() {
    return String.format("%s/%s", path, this.getIndexFileName());
  }

  /**
   * 1:1 mapping of record to file
   */
  public void write(String record) throws IOException {
    boolean hasNewLine = record.endsWith("\n");

    int rawBytesToWrite = record.length();
    if (!hasNewLine) {
      rawBytesToWrite += 1;
    }

    writer.write(record);
    if (!hasNewLine) {
      writer.newLine();
    }

    jr = new JsonRecord();
    jr.recordOffset = recordOffset;
    jr.rawBytes += rawBytesToWrite;
  }

  /**
   * Index file is simply a file which stores metadata regarding chunks written to s3
   * topic-partition-recordOffset.index.json
   */
  private void writeIndex() throws IOException {
    JSONObject recordMetadata = new JSONObject();
    recordMetadata.put("byte_length_uncompressed", jr.rawBytes);
    recordMetadata.put("first_record_offset", jr.recordOffset);

    try (FileWriter file = new FileWriter(getIndexFilePath())) {
      file.write(recordMetadata.toJSONString());
      file.close();
    }
  }

  public void close() throws IOException {
    writer.close();
    writeIndex();
  }
}