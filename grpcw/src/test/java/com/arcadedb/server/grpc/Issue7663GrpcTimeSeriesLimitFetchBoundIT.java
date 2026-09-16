/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
package com.arcadedb.server.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #7663: the server-streaming {@code TimeSeriesQuery} read {@code req.getLimit()} and then asked the engine
 * for the WHOLE range:
 *
 * <pre>
 * final boolean clientLimited = requestedLimit &gt; 0;
 * final Iterator&lt;Object[]&gt; rows = engine.iterateQuery(fromTs, toTs, columnIndices, tagFilter);
 * </pre>
 *
 * while the method's own javadoc told a reader that {@code iterateQuery} was "a lazy merge across shards, so the
 * server holds one block per shard rather than the whole range". {@code iterateQuery}'s javadoc says the opposite
 * - {@code TimeSeriesSealedStore#iterateRange} materialises every matching row of the sealed layer before the
 * iterator is returned - so a {@code limit: 10} over an unbounded range paid O(matching rows) heap before the
 * first message was emitted, exactly as {@code POST /ts/{database}/query} did before #7336.
 * <p>
 * The client-limited arm now fetches through {@code TimeSeriesEngine.queryAscending} with {@code limit + 1}: the
 * extra row still decides {@code truncated}, and the comment now says what the unlimited arm actually costs.
 * <p>
 * The residency saving is asserted at engine level by {@code Issue7336AscendingLimitTest}, which counts the blocks
 * decompressed. What this class pins is that NOTHING a client can observe changed - row count, ordering, content,
 * {@code truncated}, {@code last}, {@code runningTotalEmitted} and the ceiling's refusals - because a bounded
 * fetch is only a fix if the answer is the same one.
 *
 * @see <a href="https://github.com/ArcadeData/arcadedb/issues/7663">issue #7663</a>
 *
 * @author Roberto Franchini (r.franchini@arcadedata.com)
 */
public class Issue7663GrpcTimeSeriesLimitFetchBoundIT extends BaseGraphServerTest {

  private static final int    GRPC_PORT   = 50051;
  private static final String TYPE_NAME   = "reading";
  private static final int    MAX_TS_ROWS = 20;
  private static final int    SAMPLES     = 40;
  private static final long   BASE_TS     = 1_700_000_000_000L;
  private static final long   STEP_MS     = 1_000L;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
    GlobalConfiguration.SERVER_GRPC_TIMESERIES_MAX_RESULT_ROWS.setValue(MAX_TS_ROWS);
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel,
        new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName()));
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);

    authenticatedStub.executeCommand(command("CREATE TIMESERIES TYPE " + TYPE_NAME
        + " TIMESTAMP ts TAGS (host STRING) FIELDS (value DOUBLE)"));

    final TimeSeriesWriteRequest.Builder write = TimeSeriesWriteRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME);
    for (int i = 0; i < SAMPLES; i++)
      write.addPoints(TimeSeriesPoint.newBuilder()
          .setTimestamp(BASE_TS + i * STEP_MS)
          .putTags("host", GrpcValue.newBuilder().setStringValue(i % 2 == 0 ? "web1" : "web2").build())
          .putFields("value", GrpcValue.newBuilder().setDoubleValue(i).build()));
    assertThat(authenticatedStub.timeSeriesWrite(write.build()).getWritten()).isEqualTo(SAMPLES);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    try {
      if (channel != null) {
        channel.shutdown();
        channel.awaitTermination(5, TimeUnit.SECONDS);
      }
    } finally {
      GlobalConfiguration.SERVER_GRPC_TIMESERIES_MAX_RESULT_ROWS.reset();
      GlobalConfiguration.SERVER_PLUGINS.setValue("");
    }
  }

  /**
   * The request shape the issue names: a small limit over the widest range the client can state. It must answer
   * the OLDEST rows, in ascending timestamp order, and report the answer as cut short.
   */
  @Test
  void aSmallLimitOverAnUnboundedRangeAnswersTheOldestRows() {
    final Stream stream = collect(query().setLimit(10).build());

    assertThat(stream.rows).hasSize(10);
    assertThat(stream.truncated).as("thirty more rows exist, so the answer really was cut short").isTrue();
    assertThat(stream.last).isTrue();
    assertThat(stream.runningTotalEmitted).isEqualTo(10);

    for (int i = 0; i < stream.rows.size(); i++)
      assertThat(stream.rows.get(i)).isEqualTo(BASE_TS + i * STEP_MS);
  }

  /**
   * The boundary the extra fetched row exists for: a range holding exactly {@code limit} rows is NOT truncated,
   * and one holding {@code limit + 1} is. Off by one here and every complete answer starts claiming it was cut.
   */
  @Test
  void theLimitBoundaryDecidesTruncationExactly() {
    final Stream exact = collect(query().setLimit(5).setToTimestamp(BASE_TS + 4 * STEP_MS).build());
    assertThat(exact.rows).hasSize(5);
    assertThat(exact.truncated).isFalse();

    final Stream cut = collect(query().setLimit(5).setToTimestamp(BASE_TS + 5 * STEP_MS).build());
    assertThat(cut.rows).hasSize(5);
    assertThat(cut.truncated).isTrue();

    // One row short of the limit is a complete answer.
    final Stream under = collect(query().setLimit(5).setToTimestamp(BASE_TS + 3 * STEP_MS).build());
    assertThat(under.rows).hasSize(4);
    assertThat(under.truncated).isFalse();
  }

  /**
   * The bounded fetch has to answer the PREFIX of the unbounded one: same rows, same order. This is the assertion
   * that would catch a cap applied to the wrong end of the series.
   */
  @Test
  void theBoundedFetchAnswersThePrefixOfTheUnboundedStream() {
    final Stream bounded = collect(query().setLimit(12).build());
    final Stream whole = collect(query().setLimit(MAX_TS_ROWS).build());

    assertThat(whole.rows).hasSize(MAX_TS_ROWS);
    assertThat(bounded.rows).isEqualTo(whole.rows.subList(0, 12));
  }

  /**
   * The tag filter is applied by the bounded scan itself, so the limit counts the rows that SURVIVE it. 'web1'
   * carries the even samples, so a selective query over the whole range must not come back short.
   */
  @Test
  void theLimitCountsOnlyTheRowsThatSurviveTheTagFilter() {
    final Stream stream = collect(query()
        .setLimit(6)
        .setTags(TimeSeriesTagFilter.newBuilder()
            .putEquals("host", GrpcValue.newBuilder().setStringValue("web1").build()))
        .build());

    assertThat(stream.rows).hasSize(6);
    assertThat(stream.truncated).isTrue();
    for (int i = 0; i < stream.rows.size(); i++)
      assertThat(stream.rows.get(i)).isEqualTo(BASE_TS + 2L * i * STEP_MS);
  }

  /**
   * A field projection travels with the bound: the rows still carry the projected column and the timestamp the
   * engine prepends, and they are still the oldest of the range.
   */
  @Test
  void aProjectedQueryIsStillBoundedAndStillCorrect() {
    final Iterator<TimeSeriesQueryResult> it = authenticatedStub.timeSeriesQuery(
        query().setLimit(3).addFields("value").build());

    final List<Long> timestamps = new ArrayList<>();
    List<String> columns = List.of();
    while (it.hasNext()) {
      final TimeSeriesQueryResult message = it.next();
      if (!message.getColumnsList().isEmpty())
        columns = message.getColumnsList();
      message.getRowsList().forEach(row -> timestamps.add(timestampOf(row)));
    }

    assertThat(columns).containsExactly("ts", "value");
    assertThat(timestamps).hasSize(3);
    for (int i = 0; i < timestamps.size(); i++)
      assertThat(timestamps.get(i)).isEqualTo(BASE_TS + i * STEP_MS);
  }

  /**
   * A limit larger than the range holds returns the range and never pads it, and reports the answer as complete.
   */
  @Test
  void aLimitLargerThanTheRangeReturnsTheRangeAndIsNotTruncated() {
    final Stream stream = collect(query().setLimit(MAX_TS_ROWS).setToTimestamp(BASE_TS + 2 * STEP_MS).build());

    assertThat(stream.rows).hasSize(3);
    assertThat(stream.truncated).isFalse();
    assertThat(stream.last).isTrue();
  }

  /**
   * An empty range is a complete, empty answer: one terminal message, no rows, not truncated. The bounded fetch
   * must not turn "nothing matched" into "cut short".
   */
  @Test
  void anEmptyRangeIsCompleteRatherThanTruncated() {
    final Stream stream = collect(query().setLimit(5)
        .setFromTimestamp(BASE_TS - 100_000L).setToTimestamp(BASE_TS - 1L).build());

    assertThat(stream.rows).isEmpty();
    assertThat(stream.truncated).isFalse();
    assertThat(stream.last).isTrue();
  }

  /**
   * The ceiling contract of #7390 is untouched by the bounded fetch: a limit ABOVE the ceiling is still refused
   * before the first message, and the refusal still names the setting that applies.
   */
  @Test
  void theCeilingStillRefusesAnOverLimitRequestBeforeTheFirstMessage() {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(
        query().setLimit(MAX_TS_ROWS + 1).build());

    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class, stream::hasNext);

    assertThat(ex).isNotNull();
    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
    assertThat(ex.getStatus().getDescription()).contains("grpcTimeSeriesMaxResultRows");
  }

  /**
   * A request stating NO limit keeps the unlimited {@code iterateQuery} arm, whose ceiling still fires mid-stream
   * - a lazy walk cannot know the row count in advance. That is the arm whose comment was wrong, not its
   * behaviour, so the behaviour must be unchanged.
   */
  @Test
  void anUnlimitedRequestKeepsTheMidStreamCeiling() {
    final Iterator<TimeSeriesQueryResult> stream = authenticatedStub.timeSeriesQuery(query().build());

    final StatusRuntimeException ex = catchThrowableOfType(StatusRuntimeException.class, () -> {
      while (stream.hasNext())
        stream.next();
    });

    assertThat(ex).as("silently stopping at the cap is the truncation this ceiling must never produce").isNotNull();
    assertThat(ex.getStatus().getCode()).isEqualTo(Status.Code.RESOURCE_EXHAUSTED);
  }

  /**
   * The rows are batched exactly as before: a limit spread over several messages still ends on one terminal
   * message carrying {@code last}, and only that message's {@code truncated} means anything.
   */
  @Test
  void theBoundedStreamIsStillDeliveredInBatches() {
    final Iterator<TimeSeriesQueryResult> it = authenticatedStub.timeSeriesQuery(
        query().setLimit(9).setBatchSize(2).build());

    int messages = 0;
    int lastCount = 0;
    int rows = 0;
    while (it.hasNext()) {
      final TimeSeriesQueryResult message = it.next();
      messages++;
      rows += message.getRowsCount();
      if (message.getLast())
        lastCount++;
    }

    assertThat(rows).isEqualTo(9);
    assertThat(messages).as("a batch size of 2 over 9 rows cannot arrive in a single message").isGreaterThan(1);
    assertThat(lastCount).as("exactly one terminal message ends the stream").isEqualTo(1);
  }

  private Stream collect(final TimeSeriesQueryRequest request) {
    final Iterator<TimeSeriesQueryResult> it = authenticatedStub.timeSeriesQuery(request);
    final Stream stream = new Stream();
    while (it.hasNext()) {
      final TimeSeriesQueryResult message = it.next();
      message.getRowsList().forEach(row -> stream.rows.add(timestampOf(row)));
      if (message.getLast()) {
        stream.last = true;
        stream.truncated = message.getTruncated();
        stream.runningTotalEmitted = message.getRunningTotalEmitted();
      }
    }
    return stream;
  }

  /**
   * A row carries its values positionally and the engine always prepends the timestamp, so {@code values[0]} is
   * it - as an {@code int64}, which is how {@code GrpcTypeConverter} encodes the {@code Long} the engine hands
   * out.
   */
  private static long timestampOf(final TimeSeriesRow row) {
    return row.getValues(0).getInt64Value();
  }

  /** What a client can observe of one stream, which is the whole of what this fix must not change. */
  private static final class Stream {
    private final List<Long> rows = new ArrayList<>();
    private       boolean    last;
    private       boolean    truncated;
    private       long       runningTotalEmitted;
  }

  private TimeSeriesQueryRequest.Builder query() {
    return TimeSeriesQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setType(TYPE_NAME);
  }

  private ExecuteCommandRequest command(final String sql) {
    return ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setCommand(sql)
        .setLanguage("sql")
        .build();
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder()
        .setUsername("root")
        .setPassword(DEFAULT_PASSWORD_FOR_TESTS)
        .build();
  }
}
