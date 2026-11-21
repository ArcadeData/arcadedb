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
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12189 over gRPC: an ARRAY_OF_* property reached the client as an ArrayList of boxed numbers, on a record read and on a
 * projection alike, where the embedded API returns the primitive array. The server now tags a primitive array's list with its
 * class and the client decodes it back to the array.
 */
class Issue12189GrpcArrayPropertyIT extends BaseGrpcClientServerTest {
  private RemoteGrpcServer   grpcServer;
  private RemoteGrpcDatabase grpc;
  private RID                rid;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @BeforeEach
  void openAndPopulate() {
    grpcServer = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    grpc = new RemoteGrpcDatabase(grpcServer, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
    grpc.command("sql", "create document type Doc12189 if not exists");
    grpc.command("sql", "create property Doc12189.shorts if not exists ARRAY_OF_SHORTS");
    grpc.command("sql", "create property Doc12189.ints if not exists ARRAY_OF_INTEGERS");
    grpc.command("sql", "create property Doc12189.longs if not exists ARRAY_OF_LONGS");
    grpc.command("sql", "create property Doc12189.floats if not exists ARRAY_OF_FLOATS");
    grpc.command("sql", "create property Doc12189.doubles if not exists ARRAY_OF_DOUBLES");
    grpc.command("sql", "delete from Doc12189");
    try (final ResultSet rs = grpc.command("sql",
        "insert into Doc12189 set shorts = [1, 2], ints = [1, 2], longs = [1, 5000000000], floats = [1.5], doubles = [2.5]")) {
      rid = rs.next().getIdentity().get();
    }
  }

  @AfterEach
  void closeClient() {
    if (grpc != null)
      grpc.close();
    if (grpcServer != null)
      grpcServer.close();
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    super.endTest();
  }

  @Test
  void aRecordReadByRidHoldsThePrimitiveArrays() {
    final var doc = grpc.lookupByRID(rid, true).asDocument();
    assertArrays(doc.get("shorts"), doc.get("ints"), doc.get("longs"), doc.get("floats"), doc.get("doubles"));
  }

  @Test
  void aProjectionHoldsThePrimitiveArrays() {
    try (final ResultSet rs = grpc.query("sql", "select shorts, ints, longs, floats, doubles from " + rid)) {
      final Result row = rs.next();
      assertArrays(row.getProperty("shorts"), row.getProperty("ints"), row.getProperty("longs"), row.getProperty("floats"),
          row.getProperty("doubles"));
    }
  }

  private static void assertArrays(final Object shorts, final Object ints, final Object longs, final Object floats,
      final Object doubles) {
    assertThat(shorts).isInstanceOf(short[].class);
    assertThat((short[]) shorts).containsExactly((short) 1, (short) 2);
    assertThat(ints).isInstanceOf(int[].class);
    assertThat((int[]) ints).containsExactly(1, 2);
    assertThat(longs).isInstanceOf(long[].class);
    assertThat((long[]) longs).containsExactly(1L, 5_000_000_000L);
    assertThat(floats).isInstanceOf(float[].class);
    assertThat((float[]) floats).containsExactly(1.5F);
    assertThat(doubles).isInstanceOf(double[].class);
    assertThat((double[]) doubles).containsExactly(2.5D);
  }
}
