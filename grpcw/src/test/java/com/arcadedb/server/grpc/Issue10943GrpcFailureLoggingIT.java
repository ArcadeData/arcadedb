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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10943: a failed gRPC command was logged at SEVERE twice, also when the failure is a retryable conflict the client simply
 * repeats. It is now logged once, and at SEVERE only when the server is at fault.
 */
public class Issue10943GrpcFailureLoggingIT extends BaseGrpcServerTest {
  private final List<String> failureLines = new CopyOnWriteArrayList<>();
  private final List<Level>  failureLevels = new CopyOnWriteArrayList<>();
  private Logger             original;

  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setUp() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().build();
    stub = ArcadeDbServiceGrpc.newBlockingStub(
        ClientInterceptors.intercept(channel, new GrpcTestAuthInterceptor("root", DEFAULT_PASSWORD_FOR_TESTS, getDatabaseName())));
    original = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new Logger() {
      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
          final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
          final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
          final Object arg17) {
        record(level, message);
        original.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10,
            arg11, arg12, arg13, arg14, arg15, arg16, arg17);
      }

      @Override
      public void log(final Object requester, final Level level, final String message, final Throwable exception,
          final String context, final Object... args) {
        record(level, message);
        original.log(requester, level, message, exception, context, args);
      }

      @Override
      public void flush() {
        original.flush();
      }

      private void record(final Level level, final String message) {
        if (message != null && message.contains("ERROR in executeCommand")) {
          failureLines.add(message);
          failureLevels.add(level);
        }
      }
    });
  }

  @AfterEach
  void tearDown() throws InterruptedException {
    LogManager.instance().setLogger(original);
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aFailedCommandIsLoggedOnceAndNotAtSevere() {
    command("CREATE DOCUMENT TYPE Dup10943");
    command("CREATE PROPERTY Dup10943.id INTEGER");
    command("CREATE INDEX ON Dup10943 (id) UNIQUE");
    command("INSERT INTO Dup10943 SET id = 1");

    assertThatThrownBy(() -> command("INSERT INTO Dup10943 SET id = 1")).isInstanceOf(StatusRuntimeException.class);

    assertThat(failureLines).as("one line for the one failed command").hasSize(1);
    assertThat(failureLevels).as("a duplicated key is the caller's, not a server fault").noneMatch(l -> l == Level.SEVERE);
  }

  @Test
  void aRetryableConflictIsNotSevere() {
    assertThat(GrpcErrorMapper.failureLevel(new ConcurrentModificationException("commit conflict"))).isEqualTo(Level.FINE);
    assertThat(GrpcErrorMapper.failureLevel(new ExecutionException(new ConcurrentModificationException("commit conflict"))))
        .as("the wrapper of a transaction's executor does not change the answer").isEqualTo(Level.FINE);
    assertThat(GrpcErrorMapper.failureLevel(new DuplicatedKeyException("T[id]", "[1]", null))).isEqualTo(Level.FINE);
  }

  @Test
  void aServerFaultIsStillSevere() {
    assertThat(GrpcErrorMapper.failureLevel(new IllegalStateException("boom"))).isEqualTo(Level.SEVERE);
  }

  private void command(final String sql) {
    stub.executeCommand(ExecuteCommandRequest.newBuilder().setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setCommand(sql).build());
  }
}
