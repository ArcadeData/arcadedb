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
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #12357: the result encoder sits on the per-row path of every gRPC read, and each value it encodes asked the logger whether FINE
 * is enabled twice: once at the top of {@code toGrpcValue}, and once more in {@code dbgEnc} on the way out, to the same answer.
 * The second ask now reuses the first.
 * <p>
 * The asks are counted by the method that makes them, because the extra one cannot be seen from what the logger receives: with
 * FINE disabled neither of them logs anything.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class Issue12357GrpcEncoderAsksTheLoggerOnceTest extends BaseGrpcServerTest {
  private static final int ROWS    = 3;
  private static final int COLUMNS = 4;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Test
  void eachEncodedValueAsksTheLoggerOnce() throws Exception {
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType("Issue12357Doc");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("Issue12357Doc").set("name", "n" + i).set("age", i).set("score", 1.5d * i).set("active", i % 2 == 0).save();
    });

    final Logger previous = LogManager.instance().getLogger();
    final FineAskCounter counter = new FineAskCounter(previous);
    final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    try {
      final Metadata headers = new Metadata();
      headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
      headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
      headers.put(Metadata.Key.of("x-arcade-database", Metadata.ASCII_STRING_MARSHALLER), getDatabaseName());
      final ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub = ArcadeDbServiceGrpc.newBlockingStub(channel)
          .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
      final ExecuteQueryRequest request = ExecuteQueryRequest.newBuilder()
          .setDatabase(getDatabaseName())
          .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
          .setQuery("SELECT name, age, score, active FROM Issue12357Doc")
          .build();

      LogManager.instance().setLogger(counter);
      final ExecuteQueryResponse response = stub.executeQuery(request);

      assertThat(response.getResults(0).getRecordsCount()).isEqualTo(ROWS);
      assertThat(response.getResults(0).getRecords(0).getPropertiesCount()).isEqualTo(COLUMNS);

      assertThat(counter.askedBy("toGrpcValue")).as("the scan still reaches the encoder: one ask for each of the rows' values")
          .isEqualTo(ROWS * COLUMNS);
      assertThat(counter.askedBy("dbgEnc")).as("dbgEnc reuses the answer toGrpcValue already holds, it does not ask again").isZero();
    } finally {
      LogManager.instance().setLogger(previous);
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  /** Hands every call to {@code delegate} and counts the FINE asks made from {@link ArcadeDbGrpcService}, by the method that made them. */
  private static final class FineAskCounter implements Logger {
    private final Logger                     delegate;
    private final Map<String, AtomicInteger> asks = new ConcurrentHashMap<>();

    private FineAskCounter(final Logger delegate) {
      this.delegate = delegate;
    }

    int askedBy(final String method) {
      final AtomicInteger count = asks.get(method);
      return count == null ? 0 : count.get();
    }

    @Override
    public boolean isLoggable(final Object requester, final Level level) {
      if (level == Level.FINE) {
        // THE FIRST FRAME OF THE SERVICE ABOVE US IS THE METHOD THAT ASKED
        final String service = ArcadeDbGrpcService.class.getName();
        final String caller = StackWalker.getInstance()
            .walk(frames -> frames.filter(f -> f.getClassName().equals(service)).map(StackWalker.StackFrame::getMethodName).findFirst()
                .orElse(null));
        if (caller != null)
          asks.computeIfAbsent(caller, k -> new AtomicInteger()).incrementAndGet();
      }
      return delegate.isLoggable(requester, level);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5, final Object arg6,
        final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11, final Object arg12,
        final Object arg13, final Object arg14, final Object arg15, final Object arg16, final Object arg17) {
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11,
          arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception, final String context,
        final Object... args) {
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
