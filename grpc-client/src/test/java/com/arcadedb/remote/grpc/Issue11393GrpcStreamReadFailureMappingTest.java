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
package com.arcadedb.remote.grpc;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.Record;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.remote.RemoteException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11393: {@code iterateType} and {@code iterateBucket} stream their records through a private iterator that wrapped a
 * failed read in a bare {@code RuntimeException} around the {@code StatusException}, and dropped the thread's interrupt flag.
 * A deadline, a permission change or a closed database arriving on the second or a later batch was therefore none of the
 * types the same status maps to from a unary call. Every streaming iterator of the driver now shares one translation.
 * <p>
 * The records are large enough that the stream cannot fit in the transport's flow-control window, so the server is still
 * holding batches when the client stops reading: the failure arrives on a later batch, not on the first one, which
 * {@code callServerStreaming} already mapped.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11393GrpcStreamReadFailureMappingTest extends BaseGrpcClientServerTest {
  private static final String TYPE    = "Issue11393";
  private static final int    RECORDS = 2_000;

  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @BeforeEach
  void open() {
    final Database database = getServerDatabase(0, getDatabaseName());
    final String payload = "x".repeat(4_096);
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE);
      for (int i = 0; i < RECORDS; i++)
        database.newDocument(TYPE).set("i", i).set("payload", payload).save();
    });

    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  void close() {
    // NEVER LEAK AN INTERRUPT FLAG INTO THE NEXT TEST OF THE FORK
    Thread.interrupted();
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
  }

  /** The reported case: the call deadline expires while the stream still has batches to deliver. */
  @Test
  void aDeadlineOnALaterBatchIsATimeoutException() throws Exception {
    grpc.setTimeout(2_000);
    final Iterator<Record> iterator = grpc.iterateType(TYPE, true);
    assertThat(iterator.hasNext()).as("the first batch is read before the deadline").isTrue();
    iterator.next();

    Thread.sleep(3_000);

    assertThatThrownBy(() -> drain(iterator)).isInstanceOf(TimeoutException.class);
  }

  @Test
  void aDeadlineOnALaterBatchOfABucketIsATimeoutException() throws Exception {
    final String bucket = getServerDatabase(0, getDatabaseName()).getSchema().getType(TYPE).getBuckets(false).getFirst()
        .getName();
    grpc.setTimeout(2_000);
    final Iterator<Record> iterator = grpc.iterateBucket(bucket);
    assertThat(iterator.hasNext()).isTrue();
    iterator.next();

    Thread.sleep(3_000);

    assertThatThrownBy(() -> drain(iterator)).isInstanceOf(TimeoutException.class);
  }

  /** An interrupt is reported, and the thread's flag is still set for the caller to see. */
  @Test
  void anInterruptedReadKeepsTheInterruptFlag() {
    final Iterator<Record> iterator = grpc.iterateType(TYPE, true);
    assertThat(iterator.hasNext()).isTrue();
    iterator.next();

    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> drain(iterator)).isInstanceOf(RemoteException.class).hasMessageContaining("interrupted")
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).as("the interrupt is restored, not swallowed").isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  /** The control: with no failure the stream yields every record, across every batch. */
  @Test
  void anUninterruptedStreamYieldsEveryRecord() {
    assertThat(drain(grpc.iterateType(TYPE, true))).isEqualTo(RECORDS);
  }

  /**
   * The admin read of the same driver wrapped its failure in a bare RuntimeException too: a refused credential is now the
   * SecurityException the HTTP driver raises for the same refusal.
   */
  @Test
  void aRefusedListDatabasesIsMapped() {
    try (final RemoteGrpcServer wrong = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", "wrong-password", true,
        List.of())) {
      assertThatThrownBy(wrong::listDatabases).isInstanceOf(SecurityException.class);
    }
  }

  private static int drain(final Iterator<Record> iterator) {
    int count = 0;
    while (iterator.hasNext()) {
      iterator.next();
      ++count;
    }
    return count;
  }
}
