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
import com.arcadedb.database.Database;
import com.arcadedb.server.BaseGraphServerTest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7029: the PAGED retrieval mode wrapped the caller's SQL in an outer
 * {@code SELECT FROM (...) ORDER BY @rid}, which discarded the caller's own ORDER BY. Rows came back in RID order,
 * silently, and because the pages were cut by SKIP/LIMIT over that rewritten order the caller could not even restore
 * the intended order client-side without pulling every page.
 * <p>
 * The wrapper now adds {@code ORDER BY @rid} only when the caller asked for no order of its own; a caller-supplied
 * ORDER BY is left to do its job inside the sub-query, which the wrapper no longer re-sorts.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue7029GrpcPagedOrderByIT extends BaseGraphServerTest {

  private static final int GRPC_PORT = 50051;
  private static final int ROWS      = 12;
  private static final int PAGE_SIZE = 5;

  private static final Metadata.Key<String> USER_HEADER     = Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> PASSWORD_HEADER = Metadata.Key.of("x-arcade-password",
      Metadata.ASCII_STRING_MARSHALLER);
  private static final Metadata.Key<String> DATABASE_HEADER = Metadata.Key.of("x-arcade-database",
      Metadata.ASCII_STRING_MARSHALLER);

  private ManagedChannel                                 channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub authenticatedStub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    // "seq" runs 0..ROWS-1 in insertion (RID) order, so ordering by -seq is the exact opposite of RID order and a
    // wrapper that re-sorts by @rid is impossible to mistake for one that does not.
    final Database db = getServerDatabase(0, getDatabaseName());
    db.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        db.newVertex(VERTEX1_TYPE_NAME).set("id", 9000L + i).set("seq", i).save();
    });

    channel = ManagedChannelBuilder.forAddress("localhost", GRPC_PORT).usePlaintext().build();
    final Channel authenticatedChannel = ClientInterceptors.intercept(channel, new AuthClientInterceptor());
    authenticatedStub = ArcadeDbServiceGrpc.newBlockingStub(authenticatedChannel);
  }

  @AfterEach
  void shutdownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private class AuthClientInterceptor implements ClientInterceptor {
    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(final MethodDescriptor<ReqT, RespT> method,
        final CallOptions callOptions, final Channel next) {
      return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
        @Override
        public void start(final Listener<RespT> responseListener, final Metadata headers) {
          headers.put(USER_HEADER, "root");
          headers.put(PASSWORD_HEADER, DEFAULT_PASSWORD_FOR_TESTS);
          headers.put(DATABASE_HEADER, getDatabaseName());
          super.start(responseListener, headers);
        }
      };
    }
  }

  private DatabaseCredentials credentials() {
    return DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build();
  }

  private List<Long> streamPagedSeq(final String query) {
    final StreamQueryRequest request = StreamQueryRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(credentials())
        .setQuery(query)
        .setBatchSize(PAGE_SIZE)
        .setRetrievalMode(StreamQueryRequest.RetrievalMode.PAGED)
        .build();

    final List<Long> seq = new ArrayList<>();
    final Iterator<QueryResult> results = authenticatedStub.streamQuery(request);
    while (results.hasNext()) {
      for (final GrpcRecord record : results.next().getRecordsList()) {
        final GrpcValue value = record.getPropertiesMap().get("seq");
        assertThat(value).as("every row must carry the 'seq' property").isNotNull();
        seq.add(value.hasInt64Value() ? value.getInt64Value() : value.getInt32Value());
      }
    }
    return seq;
  }

  @Test
  void pagedModeKeepsTheCallerDescendingOrderAcrossPages() {
    // No projection, so the sub-query yields records and the outer statement really can sort them by @rid - which is
    // what used to happen. RID order here is neither ascending nor descending "seq" (the type's buckets are filled
    // round-robin), so the outer re-sort is unmistakable. 12 rows over pages of 5 means three pages, so the order has
    // to survive the page boundaries and not just hold within one page.
    final List<Long> seq = streamPagedSeq("SELECT FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL ORDER BY seq DESC");

    assertThat(seq).as("every row must be delivered exactly once across the pages").hasSize(ROWS);
    assertThat(seq)
        .as("PAGED must deliver the rows in the order the caller asked for, not in RID order")
        .isSortedAccordingTo((a, b) -> Long.compare(b, a));
    assertThat(seq.get(0)).isEqualTo(ROWS - 1L);
    assertThat(seq.get(seq.size() - 1)).isZero();
  }

  @Test
  void pagedModeKeepsTheCallerAscendingOrder() {
    final List<Long> seq = streamPagedSeq("SELECT FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL ORDER BY seq ASC");

    assertThat(seq).hasSize(ROWS);
    assertThat(seq).as("an ascending caller order must be preserved too, and it is not the RID order either").isSorted();
  }

  @Test
  void pagedModeKeepsTheCallerOrderOnAProjection() {
    // A projected column shows the fix does not depend on the outer statement being able to see @rid or the sort key:
    // the ordering is applied inside the sub-query, where both are still in scope. Note this case survived the old
    // wrapper too, by luck rather than design - @rid is null on every projected row, so the outer sort was a stable
    // no-op. It is here as a guard that the fix does not break it, not as a reproduction of the bug; the two
    // record-returning tests above are what fail against the old code.
    final List<Long> seq = streamPagedSeq(
        "SELECT seq AS seq FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL ORDER BY seq DESC");

    assertThat(seq).hasSize(ROWS);
    assertThat(seq).isSortedAccordingTo((a, b) -> Long.compare(b, a));
  }

  @Test
  void pagedModeStillPagesStablyWhenTheCallerAsksForNoOrder() {
    // Without a caller-supplied ORDER BY the wrapper keeps ordering by @rid, which is what makes SKIP/LIMIT paging
    // sound. RID order is not insertion order - the type's buckets are filled round-robin - so what is asserted here
    // is what @rid buys: every row delivered exactly once, and the same total order on every run, so the pages of one
    // scan cannot drop or duplicate a row.
    final String query = "SELECT FROM " + VERTEX1_TYPE_NAME + " WHERE seq IS NOT NULL";

    final List<Long> seq = streamPagedSeq(query);
    final List<Long> expected = new ArrayList<>();
    for (long i = 0; i < ROWS; i++)
      expected.add(i);

    assertThat(seq).as("with no caller order, @rid paging must deliver every row exactly once")
        .hasSize(ROWS)
        .containsExactlyInAnyOrderElementsOf(expected);
    assertThat(streamPagedSeq(query)).as("@rid gives the pages a stable total order, so a re-run repeats it")
        .containsExactlyElementsOf(seq);
  }
}
