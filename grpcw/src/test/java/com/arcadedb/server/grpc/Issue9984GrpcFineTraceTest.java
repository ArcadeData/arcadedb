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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #9984: the gRPC service's fine grained wire trace was guarded by {@code LogManager.isDebugEnabled()}, a flag with no
 * writer outside a test, so no configuration could turn it on. The guards now follow the logging configuration: with
 * the service's logger at {@code FINE} the conversion traces are published. They describe values by their shape only
 * - the content of a record stays out of the log unless {@code arcadedb.grpc.log.showString} opts in - since setting a
 * logger to FINE to watch the wire must not start copying user data into it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9984GrpcFineTraceTest extends BaseGrpcServerTest {
  private static final String SECRET = "Issue9984-content-that-must-not-reach-the-log";

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Test
  void theConversionTraceFollowsTheLoggerLevelAndKeepsValuesOut() throws Exception {
    final Logger jul = Logger.getLogger(ArcadeDbGrpcService.class.getName());
    final Level previousLevel = jul.getLevel();
    final List<LogRecord> published = new CopyOnWriteArrayList<>();
    final Handler capture = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        published.add(record);
      }

      @Override
      public void flush() {
      }

      @Override
      public void close() {
      }
    };
    capture.setLevel(Level.ALL);

    // The value lives in a record, not in the query text: the statement itself is traced at FINE on purpose
    final Database database = getServerDatabase(0, getDatabaseName());
    database.getSchema().createDocumentType("Issue9984Doc");
    database.transaction(() -> database.newDocument("Issue9984Doc").set("secret", SECRET).save());

    final ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    jul.addHandler(capture);
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
          .setQuery("SELECT secret FROM Issue9984Doc")
          .build();

      // at the default level the trace stays closed
      jul.setLevel(Level.INFO);
      assertThat(stub.executeQuery(request).getResultsList()).isNotEmpty();
      assertThat(fineRecords(published)).isEmpty();

      // at FINE it opens, which no configuration could do while the guard read the unset flag
      jul.setLevel(Level.FINE);
      assertThat(LogManager.instance().isLoggable(ArcadeDbGrpcService.class, Level.FINE)).isTrue();
      final ExecuteQueryResponse response = stub.executeQuery(request);
      assertThat(response.getResults(0).getRecords(0).getPropertiesMap().get("secret").getStringValue()).isEqualTo(SECRET);

      final List<String> trace = fineRecords(published);
      assertThat(trace).isNotEmpty();
      assertThat(trace).anyMatch(message -> message.contains("secret") && message.contains("String(" + SECRET.length() + ")"));
      assertThat(trace).noneMatch(message -> message.contains(SECRET));
    } finally {
      jul.removeHandler(capture);
      jul.setLevel(previousLevel);
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static List<String> fineRecords(final List<LogRecord> published) {
    return published.stream().filter(r -> r.getLevel() == Level.FINE).map(LogRecord::getMessage).toList();
  }
}
