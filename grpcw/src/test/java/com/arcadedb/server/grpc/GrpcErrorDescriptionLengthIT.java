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
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Issue #12567: a statement whose error text was longer than the metadata a gRPC client receives was answered with a bare
 * {@code RESOURCE_EXHAUSTED} instead of the error. Sent through a real server and a stock grpc-java client, whose default
 * limit on the metadata it receives is 8 KB.
 */
public class GrpcErrorDescriptionLengthIT extends BaseGrpcServerTest {
  private ManagedChannel                                  channel;
  private ArcadeDbServiceGrpc.ArcadeDbServiceBlockingStub stub;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GrpcServer:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void setupGrpcClient() {
    channel = ManagedChannelBuilder.forAddress("localhost", getServerGrpcPort()).usePlaintext().maxInboundMessageSize(8 * 1024 * 1024)
        .build();
    final Metadata headers = new Metadata();
    headers.put(Metadata.Key.of("x-arcade-user", Metadata.ASCII_STRING_MARSHALLER), "root");
    headers.put(Metadata.Key.of("x-arcade-password", Metadata.ASCII_STRING_MARSHALLER), DEFAULT_PASSWORD_FOR_TESTS);
    stub = ArcadeDbServiceGrpc.newBlockingStub(channel).withInterceptors(MetadataUtils.newAttachHeadersInterceptor(headers));
  }

  @AfterEach
  void teardownGrpcClient() throws InterruptedException {
    if (channel != null) {
      channel.shutdown();
      channel.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void aSyntaxErrorInAOneMegabyteStatementIsAnsweredWithTheError() {
    final String statement = "SELECT ) " + "x".repeat(1_000_000);

    // The whole syntax error fits: the quote and the caret under it
    assertThat(refusal(statement, Status.Code.INVALID_ARGUMENT)).contains("SQL syntax error").contains("\n       ^");
  }

  @Test
  void aSyntaxErrorInALongNonAsciiStatementIsAnsweredWithTheError() {
    // Every CJK character takes 9 bytes on the wire once UTF-8 and percent-encoded
    final String statement = "SELECT ) '" + "\u8868".repeat(100_000) + "'";

    assertThat(refusal(statement, Status.Code.INVALID_ARGUMENT)).contains("SQL syntax error").endsWith("... (truncated)");
  }

  @Test
  void aMissingTypeWithALongNameIsAnsweredWithTheError() {
    // Not a syntax error: the engine's own message names the type, as long as the statement
    final String statement = "SELECT FROM " + "x".repeat(100_000);

    assertThat(refusal(statement, Status.Code.NOT_FOUND)).contains("Type with name 'xxx").endsWith("... (truncated)");
  }

  /**
   * The description the statement is refused with, after checking the status it is refused with.
   */
  private String refusal(final String statement, final Status.Code code) {
    final ExecuteCommandRequest request = ExecuteCommandRequest.newBuilder()
        .setDatabase(getDatabaseName())
        .setCredentials(DatabaseCredentials.newBuilder().setUsername("root").setPassword(DEFAULT_PASSWORD_FOR_TESTS).build())
        .setLanguage("sql")
        .setCommand(statement)
        .build();

    final StatusRuntimeException e = catchThrowableOfType(StatusRuntimeException.class,
        () -> stub.withDeadlineAfter(60, TimeUnit.SECONDS).executeCommand(request));
    assertThat(e).as("the statement is refused").isNotNull();
    assertThat(e.getStatus().getCode()).as(e.getStatus().toString()).isEqualTo(code);
    return e.getStatus().getDescription();
  }
}
