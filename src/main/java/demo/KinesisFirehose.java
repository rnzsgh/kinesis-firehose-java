/**
 * Any code, applications, scripts, templates, proofs of concept,
 * documentation and other items are provided for illustration purposes only.
 *
 * Copyright 2017 Ryan
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package demo;

// Aws
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResult;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;

// Guava
import com.google.common.util.concurrent.RateLimiter;

// Java
import java.util.Timer;
import java.util.TimerTask;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

public class KinesisFirehose {

  private static final int RECORD_LENGTH = 1000;
  private static final int RECORD_BATCH_COUNT = 500;
  private static final int MAX_QUEUE_SIZE = 100000;
  private static final long READ_TIMEOUT_MS = 250;
  private static final long BATCH_TIMEOUT_MS = 2000;
  private static final int LOADER_THREADS = 7;
  private static final int RECORDS_PER_SECOND = 50000;

  private static final String FIREHOSE_STREAM_NAME = "test";

  public static void main(final String [] pArgs) throws Exception {
    final LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    final RateLimiter rateLimiter = RateLimiter.create(RECORDS_PER_SECOND);

    // Assumes you are using an IAM role for credentials or have ~/.aws/credentials in place
    final AmazonKinesisFirehose firehoseClient = AmazonKinesisFirehoseClientBuilder.defaultClient();

    for (int idx=0; idx < LOADER_THREADS; idx++) {
      new Loader(
          queue,
          firehoseClient,
          rateLimiter,
          READ_TIMEOUT_MS,
          BATCH_TIMEOUT_MS,
          RECORD_BATCH_COUNT,
          FIREHOSE_STREAM_NAME
      ).start();
    }

    new Producer(queue, RECORD_LENGTH).start();

    Thread.sleep(Integer.MAX_VALUE);
  }

  private static class Producer extends Thread {

    private final LinkedBlockingQueue<Record> _queue;
    //private final Timer _timer = new Timer(true);
    private final byte[] _testData;

    public Producer(final LinkedBlockingQueue<Record> pQueue, final int pRecordLength) throws Exception {
      _queue = pQueue;
      _testData = new String(new byte[pRecordLength]).getBytes("UTF-8");
      //_timer.scheduleAtFixedRate(this, 500, 1);
    }

    @Override public void run() {
      try {

        while (true) {
          _queue.put(new Record().withData(ByteBuffer.wrap(_testData)));
        }

      } catch (final Throwable t) { throw new IllegalStateException(t); }
    }
  }

  private static class Loader extends Thread {

    private final AmazonKinesisFirehose _firehoseClient;
    private final LinkedBlockingQueue<Record> _queue;
    private final long _readTimeout;
    private final long _batchTimeout;
    private final int _batchSize;
    private final String _streamName;
    private final RateLimiter _rateLimiter;

    public Loader(final LinkedBlockingQueue<Record> pQueue,
                  final AmazonKinesisFirehose pFirehoseClient,
                  final RateLimiter pRateLimiter,
                  final long pReadTimeout,
                  final long pBatchTimeout,
                  final int pBatchSize,
                  final String pStreamName)
    {
      _queue = pQueue;
      _firehoseClient = pFirehoseClient;
      _rateLimiter = pRateLimiter;
      _readTimeout = pReadTimeout;
      _batchTimeout = pBatchTimeout;
      _batchSize = pBatchSize;
      _streamName = pStreamName;
    }

    private void sendRecords(final LinkedList<Record> pRecords) throws Exception {

      try {

        _rateLimiter.acquire(pRecords.size());

        final LinkedList<Record> failed = new LinkedList<>();

        final PutRecordBatchRequest request = new PutRecordBatchRequest();
        request.setDeliveryStreamName(_streamName);
        request.setRecords(pRecords);

        final PutRecordBatchResult result  = _firehoseClient.putRecordBatch(request);

        int idx = 0;
        for (final PutRecordBatchResponseEntry entry : result.getRequestResponses()) {
          if (entry.getErrorCode() != null) {
            failed.add(pRecords.get(idx));
          }
          idx++;
        }

        pRecords.clear();

        if (failed.size() > 0) {
          System.out.println("Failed count: " + failed.size());
          pRecords.addAll(failed);
          sendRecords(pRecords);
        }

      } catch (final CancellationException ce) {
        ce.printStackTrace();
        sendRecords(pRecords);
      } catch (final ExecutionException ee) {
        ee.printStackTrace();
        sendRecords(pRecords);
      }
    }

    @Override public void run() {
      try {
        LinkedList<Record> records = new LinkedList<>();

        long lastFlush = System.currentTimeMillis();

        long flushCount = 0;

        while (true) {

          final Record record = _queue.poll(_readTimeout, TimeUnit.MILLISECONDS);

          if (record != null) {
            records.add(record);
          }

          if (records.size() >= _batchSize || ((System.currentTimeMillis() - lastFlush > _batchTimeout) && records.size() > 0)) {
            flushCount++;
            System.out.println("Flushing: " + flushCount + " - records: " + records.size());

            sendRecords(records);

            lastFlush = System.currentTimeMillis();
            records.clear();
          }
        }
      } catch (final Throwable t) { throw new IllegalStateException(t); }
    }
  }
}

