package com.deviantart.kafka_connect_s3;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.S3ClientOptions;


public class S3JsonSinkTask extends SinkTask {

  private static final Logger log = LoggerFactory.getLogger(S3JsonSinkTask.class);

  private Map<String, String> config;

  private S3Writer s3;

  public S3JsonSinkTask() {
  }

  @Override
  public String version() {
    return S3SinkConnectorConstants.VERSION;
  }

  @Override
  public void start(Map<String, String> props) throws ConnectException {
    config = props;

    String bucket = config.get("s3.bucket");
    String prefix = config.get("s3.prefix");
    if (bucket == null || bucket == "") {
      throw new ConnectException("S3 bucket must be configured");
    }
    if (prefix == null) {
      prefix = "";
    }

    // Use default credentials provider that looks in Env + Java properties + profile + instance role
    AmazonS3 s3Client = new AmazonS3Client();

    // If worker config sets explicit endpoint override (e.g. for testing) use that
    String s3Endpoint = config.get("s3.endpoint");
    if (s3Endpoint != null && s3Endpoint != "") {
      s3Client.setEndpoint(s3Endpoint);
    }
    Boolean s3PathStyle = Boolean.parseBoolean(config.get("s3.path_style"));
    if (s3PathStyle) {
      s3Client.setS3ClientOptions(new S3ClientOptions().withPathStyleAccess(true));
    }

    s3 = new S3Writer(bucket, prefix, s3Client);
  }

  @Override
  public void stop() throws ConnectException {
    // We could try to be smart and flush buffer files to be resumed
    // but for now we just start again from where we got to in S3 and overwrite any
    // buffers on disk.
  }

  @Override
  public void put(Collection<SinkRecord> records) throws ConnectException {
    for (SinkRecord record : records) {
      try {
        String topic = record.topic();
        int partition = record.kafkaPartition();
        TopicPartition tp = new TopicPartition(topic, partition);
        JsonFileWriter buffer = createNextBlockWriter(new TopicPartition(topic, partition), record.kafkaOffset());
        if (buffer == null) {
          log.error("Trying to put {} records to partition {} which doesn't exist yet", records.size(), tp);
          throw new ConnectException("Trying to put records for a topic partition that has not be assigned");
        }
        buffer.write(record.value().toString());
        buffer.close();

        s3.uploadChunk(buffer.getDataFilePath(), buffer.getIndexFilePath(), tp);
        log.info("Successfully uploaded chunk for {} at kafka offset {}", tp, record.kafkaOffset());
      } catch (IOException e) {
        throw new RetriableException("Failed to write to buffer", e);
      }
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  private JsonFileWriter createNextBlockWriter(TopicPartition tp, long nextOffset) throws ConnectException, IOException {
    String name = String.format("%s-%05d", tp.topic(), tp.partition());
    String path = config.get("local.buffer.dir");
    if (path == null) {
      throw new ConnectException("No local buffer file path configured");
    }
    return new JsonFileWriter(name, path, nextOffset);
  }
}
