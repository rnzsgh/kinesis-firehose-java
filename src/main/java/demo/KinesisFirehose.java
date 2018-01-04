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
 *
 * Bastion stack creation prerequisite:  first create an EC2 key pair and a VPC stack.
 * For details about how to connect to a Linux instance in a private subnet via the
 * bastion, see the following AWS blog post:
 * https://aws.amazon.com/blogs/security/securely-connect-to-linux-instances-running-in-a-private-amazon-vpc/
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
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.stream.Collectors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

public class KinesisFirehose {

  private static final int RECORD_COUNT = 100000000;
  private static final int RECORD_LENGTH = 1000;
  private static final int RECORD_BATCH_COUNT = 500;
  private static final long READ_TIMEOUT_MS = 250;
  private static final long BATCH_TIMEOUT_MS = 1000;
  private static final long BUFFER_TIMEOUT_MS = 250;
  private static final int MAX_FUTURES = 5;
  private static final int LOADER_THREADS = 12;
  private static final String FIREHOSE_STREAM_NAME = "test";

  public static void main(final String [] pArgs) throws Exception {
    final LinkedBlockingQueue<Record> queue = new LinkedBlockingQueue<>();

    final CountDownLatch countDownLatch = new CountDownLatch(LOADER_THREADS);

    final AtomicBoolean running = new AtomicBoolean(true);

    for (int idx=0; idx < LOADER_THREADS; idx++) {
      new Loader(
          queue,
          countDownLatch,
          READ_TIMEOUT_MS,
          BATCH_TIMEOUT_MS,
          RECORD_BATCH_COUNT,
          MAX_FUTURES,
          FIREHOSE_STREAM_NAME,
          running
      ).start();
    }

    new Producer(queue, RECORD_COUNT, RECORD_LENGTH, running).start();

    countDownLatch.await();
  }

  private static class Producer extends Thread {

    private final LinkedBlockingQueue<Record> _queue;
    private final int _recordCount;
    private final int _recordLength;
    private final AtomicBoolean _running;

    public Producer(final LinkedBlockingQueue<Record> pQueue,
                    final int pRecordCount,
                    final int pRecordLength,
                    final AtomicBoolean pRunning)
    {
      _queue = pQueue;
      _recordCount = pRecordCount;
      _recordLength = pRecordLength;
      _running = pRunning;
    }

    @Override public void run() {
      final String testData = new String(new byte[_recordLength]);
      try {
        for (int idx=0; idx < _recordCount; idx++) {
          // Blocking call if queue is full
          _queue.put(new Record().withData(ByteBuffer.wrap(testData.getBytes("UTF-8"))));
        }

        // Tell the loader that we are done
        _running.set(false);
        System.out.println("Producer is done");
      } catch (final Throwable t) { throw new IllegalStateException(t); }
    }
  }

  private static class Loader extends Thread {

    private final LinkedBlockingQueue<Record> _queue;
    private final CountDownLatch _countDownLatch;
    private final long _readTimeout;
    private final long _batchTimeout;
    private final int _batchSize;
    private final String _streamName;
    private final AtomicBoolean _running;
    private final int _maxFutures;

    // Assumes you are using an IAM role for credentials or have ~/.aws/credentials in place
    //private final AmazonKinesisFirehose _firehoseClient = AmazonKinesisFirehoseClientBuilder.defaultClient();
    private final AmazonKinesisFirehoseAsync _firehoseClient = AmazonKinesisFirehoseAsyncClientBuilder.defaultClient();

    public Loader(final LinkedBlockingQueue<Record> pQueue,
                  final CountDownLatch pCountDownLatch,
                  final long pReadTimeout,
                  final long pBatchTimeout,
                  final int pBatchSize,
                  final int pMaxFutures,
                  final String pStreamName,
                  final AtomicBoolean pRunning)
    {
      _queue = pQueue;
      _countDownLatch = pCountDownLatch;
      _readTimeout = pReadTimeout;
      _batchTimeout = pBatchTimeout;
      _batchSize = pBatchSize;
      _maxFutures = pMaxFutures;
      _streamName = pStreamName;
      _running = pRunning;
    }

    /*
    private boolean checkFutures(final LinkedList<Future<PutRecordBatchResult>> pFutures) throws Exception {
      boolean same = true;
      for (Iterator<Future<PutRecordBatchResult>> iter = pFutures.iterator(); iter.hasNext();) {
        final Future<PutRecordBatchResult> future = iter.next();
        if (future.isDone()) {
          final PutRecordBatchResult result = future.get();

          if (result.getFailedPutCount() > 0) {
            System.out.println("Failed count: " + result.getFailedPutCount());
            // TODO: Handle error conditions - loop through the  getRequestResponses()
            // and process the failed records. This requires caching the records in memory
            // until there is a confirmation that and then removing it
          }

          iter.remove();
          same = false;
          System.out.println("remove done - size: " + pFutures.size());
        }
      }

      return same;
    }
    */

    private void addRecordsToQueue(final LinkedList<Record> pRecords) {
      pRecords.forEach(r -> {
        try { _queue.put(r); } catch (final Throwable t) { throw new IllegalStateException(t); }
      });
    }

    @Override public void run() {
      try {
        final LinkedList<Record> records = new LinkedList<>();

        long lastFlush = System.currentTimeMillis();

        long flushCount = 0;

        while (_running.get()) {

          final Record record = _queue.poll(_readTimeout, TimeUnit.MILLISECONDS);

          if (record != null) {
            records.add(record);
          }

          if (records.size() >= _batchSize || ((System.currentTimeMillis() - lastFlush > _batchTimeout) && records.size() > 0)) {
            flushCount++;
            System.out.println("Flushing: " + flushCount);

            // Flush
            final PutRecordBatchRequest request = new PutRecordBatchRequest();
            request.setDeliveryStreamName(_streamName);
            request.setRecords(records);

            try {

              final PutRecordBatchResult result  = _firehoseClient.putRecordBatchAsync(request).get();

              if (result.getFailedPutCount() > 0) {
                System.out.println("failed count: " + result.getFailedPutCount());
              }

              int idx = 0;
              for (final PutRecordBatchResponseEntry entry : result.getRequestResponses()) {

                // TODO: Check error codes
                if (entry.getErrorCode() != null) {
                  _queue.put(records.get(idx));
                }
                idx++;
              }

              lastFlush = System.currentTimeMillis();

            } catch (final CancellationException ce) {
              addRecordsToQueue(records);
              ce.printStackTrace();
            } catch (final ExecutionException ee) {
              addRecordsToQueue(records);
              ee.printStackTrace();
            }

            records.clear();
          }
        }

        // TODO: Make sure the queue is empty

        System.out.println("Loader is done");
        _countDownLatch.countDown();

      } catch (final Throwable t) { throw new IllegalStateException(t); }
    }
  }
}

