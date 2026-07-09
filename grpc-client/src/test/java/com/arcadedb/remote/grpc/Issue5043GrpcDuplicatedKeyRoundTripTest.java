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
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end round-trip for issue #5043: a duplicate-key violation raised on the server must reconstruct
 * on the client - across the real gRPC transport - as a {@link DuplicatedKeyException} carrying the index
 * name and keys, and it must NOT be a {@link NeedRetryException} (a permanent conflict is not retryable).
 * <p>
 * The unit tests {@code GrpcErrorMapperTest} and {@code Issue5043GrpcErrorTypeReconstructionTest} exercise
 * each mapper side in isolation; this test locks in the full wire path (server {@code onError} with the
 * metadata trailer -> gRPC transport -> client {@code Status.trailersFromThrowable} reconstruction) so a
 * future change to trailer wiring cannot silently break production while both unit suites still pass.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Issue5043GrpcDuplicatedKeyRoundTripTest extends BaseGraphServerTest {

  static final String TYPE = "PersonUniq";

  private RemoteGrpcServer grpcServer;

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

  @BeforeAll
  void ensureServer() {
    grpcServer = new RemoteGrpcServer("localhost", 50051, "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
  }

  @AfterAll
  void teardownServer() {
    if (grpcServer != null)
      grpcServer.close();
  }

  @BeforeEach
  void createSchema() {
    try (final RemoteGrpcDatabase db = newConnection()) {
      db.command("sql", "CREATE VERTEX TYPE `" + TYPE + "` IF NOT EXISTS");
      db.command("sql", "CREATE PROPERTY `" + TYPE + "`.email STRING");
      db.command("sql", "CREATE INDEX IF NOT EXISTS ON `" + TYPE + "` (email) UNIQUE");
    }
  }

  private RemoteGrpcDatabase newConnection() {
    return new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @Test
  void duplicatedKeyReconstructsAsNonRetryableAcrossTheWire() {
    final String email = "roundtrip@arcadedb.com";

    try (final RemoteGrpcDatabase db = newConnection()) {
      // First insert of the unique key succeeds.
      db.begin();
      final MutableVertex first = db.newVertex(TYPE);
      first.set("email", email);
      first.save();
      db.commit();

      // Second insert of the same unique key must surface a duplicate-key error over gRPC.
      db.begin();
      final MutableVertex dup = db.newVertex(TYPE);
      dup.set("email", email);

      // The violation is raised either at save() (createRecord path) or at commit() (commitTransaction
      // path); both are routed through GrpcErrorMapper, so the client rebuilds the exact type regardless.
      assertThatThrownBy(() -> {
        dup.save();
        db.commit();
      })
          .isInstanceOf(DuplicatedKeyException.class)
          // Permanent conflict: transaction() retry-on-NeedRetryException must NOT fire.
          .isNotInstanceOf(NeedRetryException.class);

      try {
        db.rollback();
      } catch (final Exception ignore) {
        // transaction may already be closed by the failed commit
      }
    }

    // Only the first record survived: the duplicate was rejected, not retried into a second row.
    try (final RemoteGrpcDatabase verify = newConnection()) {
      final Number count = verify.query("sql", "SELECT count(*) AS c FROM `" + TYPE + "` WHERE email = ?", email)
          .next().getProperty("c");
      assertThat(count.longValue()).isEqualTo(1L);
    }
  }
}
