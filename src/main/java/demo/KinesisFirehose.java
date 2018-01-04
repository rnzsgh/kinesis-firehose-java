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
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsync;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseAsyncClientBuilder;
import com.amazonaws.services.kinesisfirehose.model.Record;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchResponseEntry;

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
  private static final int MAX_QUEUE_SIZE = 30000;
  private static final long READ_TIMEOUT_MS = 250;
  private static final long BATCH_TIMEOUT_MS = 1000;
  private static final long BUFFER_TIMEOUT_MS = 250;
  private static final int LOADER_THREADS = 4;
  private static final int REQUESTS_PER_SECOND = 5000;
  private static final String FIREHOSE_STREAM_NAME = "test";

  public static void main(final String [] pArgs) throws Exception {
    final LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    // Assumes you are using an IAM role for credentials or have ~/.aws/credentials in place
    final AmazonKinesisFirehoseAsync firehoseClient = AmazonKinesisFirehoseAsyncClientBuilder.defaultClient();

    for (int idx=0; idx < LOADER_THREADS; idx++) {
      new Loader(
          queue,
          firehoseClient,
          READ_TIMEOUT_MS,
          BATCH_TIMEOUT_MS,
          RECORD_BATCH_COUNT,
          FIREHOSE_STREAM_NAME
      ).start();
    }

    for (int idx=0; idx < REQUESTS_PER_SECOND / 1000; idx++) {
      new Producer(queue, RECORD_LENGTH);
    }

    Thread.sleep(Integer.MAX_VALUE);
  }

  private static class Producer extends TimerTask {

    private final LinkedBlockingQueue<Record> _queue;
    private final Timer _timer = new Timer(true);
    private final byte[] _testData;

    public Producer(final LinkedBlockingQueue<Record> pQueue, final int pRecordLength) throws Exception {
      _queue = pQueue;
      _testData = new String(new byte[pRecordLength]).getBytes("UTF-8");
      _timer.scheduleAtFixedRate(this, 500, 1);
    }

    @Override public void run() {
      try { _queue.put(new Record().withData(ByteBuffer.wrap(_testData)));
      } catch (final Throwable t) { throw new IllegalStateException(t); }
    }
  }

  private static class Loader extends Thread {

    private final AmazonKinesisFirehoseAsync _firehoseClient;
    private final LinkedBlockingQueue<Record> _queue;
    private final long _readTimeout;
    private final long _batchTimeout;
    private final int _batchSize;
    private final String _streamName;

    public Loader(final LinkedBlockingQueue<Record> pQueue,
                  final AmazonKinesisFirehoseAsync pFirehoseClient,
                  final long pReadTimeout,
                  final long pBatchTimeout,
                  final int pBatchSize,
                  final String pStreamName)
    {
      _queue = pQueue;
      _firehoseClient = pFirehoseClient;
      _readTimeout = pReadTimeout;
      _batchTimeout = pBatchTimeout;
      _batchSize = pBatchSize;
      _streamName = pStreamName;
    }

    /**
     * Returns records unable to send.
     */
    private LinkedList<Record> sendRecords(final LinkedList<Record> pRecords) throws Exception {

      try {

        final LinkedList<Record> failed = new LinkedList<>();

        final PutRecordBatchRequest request = new PutRecordBatchRequest();
        request.setDeliveryStreamName(_streamName);
        request.setRecords(pRecords);

        final PutRecordBatchResult result  = _firehoseClient.putRecordBatchAsync(request).get();

        int idx = 0;
        for (final PutRecordBatchResponseEntry entry : result.getRequestResponses()) {

          if (entry.getErrorCode() != null) {
            failed.add(pRecords.get(idx));
          }
          idx++;
        }

        if (failed.size() > 0) {
          System.out.println("Failed count: " + failed.size());
        }

        return failed;

      } catch (final CancellationException ce) {
        ce.printStackTrace();
        return (LinkedList<Record>)pRecords.stream().collect(Collectors.toList());
      } catch (final ExecutionException ee) {
        ee.printStackTrace();
        return (LinkedList<Record>)pRecords.stream().collect(Collectors.toList());
      }
    }

    @Override public void run() {
      try {
        final LinkedList<Record> records = new LinkedList<>();

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

            LinkedList<Record> failed = records;

            do {

              failed = sendRecords(records);

            } while (failed.size() > 0);

            lastFlush = System.currentTimeMillis();
            records.clear();
          }
        }
      } catch (final Throwable t) { throw new IllegalStateException(t); }
    }
  }
}

