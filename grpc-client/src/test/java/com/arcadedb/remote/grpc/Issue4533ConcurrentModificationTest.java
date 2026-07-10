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
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
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
 * Reproduces issue #4533: a write-write conflict between two concurrent REPEATABLE_READ transactions must raise a
 * {@link ConcurrentModificationException} on gRPC, exactly as it already does on HTTP.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class Issue4533ConcurrentModificationTest extends BaseGraphServerTest {

  static final String TYPE = "SimpleVertex";

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
    }
  }

  private RemoteGrpcDatabase newConnection() {
    return new RemoteGrpcDatabase(grpcServer, "localhost", 50051, 2480, getDatabaseName(), "root", DEFAULT_PASSWORD_FOR_TESTS);
  }

  @Test
  void concurrentModificationIsDetectedOnGrpc() {
    final RemoteGrpcDatabase t1 = newConnection();
    final RemoteGrpcDatabase t2 = newConnection();
    try {
      // Create the record and commit it (version 1 on disk).
      t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      final MutableVertex created = t1.newVertex(TYPE);
      created.set("s", "init concurrent test");
      created.save();
      final RID rid = created.getIdentity();
      t1.commit();

      // Both transactions read the same record at version 1.
      t2.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      final Vertex readByT2 = t2.lookupByRID(rid, true).asVertex();

      // t1 modifies and commits the record (now version 2 on disk).
      t1.begin(Database.TRANSACTION_ISOLATION_LEVEL.REPEATABLE_READ);
      final Vertex readByT1 = t1.lookupByRID(rid, true).asVertex();
      final MutableVertex modifiedByT1 = readByT1.modify();
      modifiedByT1.set("s", "concurrent t1");
      modifiedByT1.save();
      t1.commit();

      // t2 modifies the stale record (read at version 1) and commits: must conflict.
      final MutableVertex modifiedByT2 = readByT2.modify();
      modifiedByT2.set("s", "concurrent t2");
      modifiedByT2.save();

      assertThatThrownBy(t2::commit).isInstanceOf(ConcurrentModificationException.class);

      // The value committed by t1 must survive.
      try (final RemoteGrpcDatabase verify = newConnection()) {
        final Vertex v = verify.lookupByRID(rid, true).asVertex();
        assertThat(v.getString("s")).isEqualTo("concurrent t1");
      }
    } finally {
      try {
        t1.rollback();
      } catch (final Exception ignore) {
      }
      try {
        t2.rollback();
      } catch (final Exception ignore) {
      }
      t1.close();
      t2.close();
    }
  }
}
