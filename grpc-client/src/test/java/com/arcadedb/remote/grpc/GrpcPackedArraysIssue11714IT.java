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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Issue #11714, through the Java client: a {@code float[]} parameter or property travels packed and is stored as the
 * {@code ARRAY_OF_FLOATS} the vector index and functions use, and a stored vector reads back as the same {@code float[]},
 * bit for bit, inside a client transaction too.
 */
public class GrpcPackedArraysIssue11714IT extends BaseGrpcClientServerTest {
  private static final String TYPE  = "GrpcVec11714";
  private static final String INDEX = TYPE + "[embedding]";

  private static final float[] SPECIAL = { Float.NaN, -0.0f, 0.0f, Float.POSITIVE_INFINITY };

  private RemoteGrpcServer   server;
  private RemoteGrpcDatabase database;

  @Override
  public void setTestConfiguration() {
    super.setTestConfiguration();
    GlobalConfiguration.SERVER_PLUGINS.setValue("GRPC:com.arcadedb.server.grpc.GrpcServerPlugin");
  }

  @Override
  protected void populateDatabase() {
    super.populateDatabase();
    final Database db = getDatabase(0);
    db.transaction(() -> {
      db.command("sql", "CREATE DOCUMENT TYPE " + TYPE + " BUCKETS 1");
      db.command("sql", "CREATE PROPERTY " + TYPE + ".name STRING");
      db.command("sql", "CREATE PROPERTY " + TYPE + ".embedding ARRAY_OF_FLOATS");
      db.command("sql", "CREATE INDEX ON " + TYPE + " (embedding) LSM_VECTOR METADATA { dimensions: 3, similarity: 'COSINE' }");
      db.command("sql", "CREATE DOCUMENT TYPE Special11714");
    });
  }

  @BeforeEach
  @Override
  public void beginTest() {
    super.beginTest();
    server = new RemoteGrpcServer("localhost", getServerGrpcPort(), "root", DEFAULT_PASSWORD_FOR_TESTS, true, List.of());
    database = new RemoteGrpcDatabase(server, "localhost", getServerGrpcPort(), getServerHttpPort(), getDatabaseName(), "root",
        DEFAULT_PASSWORD_FOR_TESTS);
  }

  @AfterEach
  @Override
  public void endTest() {
    GlobalConfiguration.SERVER_PLUGINS.setValue("");
    if (database != null)
      database.close();
    if (server != null)
      server.close();
    super.endTest();
  }

  @Test
  void floatParametersAreStoredAsVectorsTheIndexFinds() {
    // The first call cannot know yet whether the server decodes packed arrays: it sends the vector as a list, and its
    // response tells the client that every later one may go packed
    assertThat(database.sendsPackedArrays()).isFalse();
    database.command("sql", "INSERT INTO " + TYPE + " SET name = 'near', embedding = :v", Map.of("v", new float[] { 1f, 0f, 0f }));
    assertThat(database.sendsPackedArrays()).isTrue();
    database.command("sql", "INSERT INTO " + TYPE + " SET name = 'mid', embedding = :v", Map.of("v", new float[] { 0.9f, 0.1f, 0f }));
    database.command("sql", "INSERT INTO " + TYPE + " SET name = 'far', embedding = :v", Map.of("v", new float[] { 0f, 1f, 0f }));

    try (final ResultSet rs = getServerDatabase(0, getDatabaseName()).query("sql", "SELECT embedding FROM " + TYPE + " WHERE name = 'mid'")) {
      assertThat(rs.next().<Object>getProperty("embedding")).isEqualTo(new float[] { 0.9f, 0.1f, 0f });
    }

    // The packed query vector reaches the index and the similarity function as the float[] they take
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql",
        "SELECT name, vector.cosineSimilarity(embedding, :q) AS s FROM (SELECT expand(vector.neighbors('" + INDEX + "', :q, 2)))",
        Map.of("q", new float[] { 1f, 0f, 0f }))) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        names.add(row.getProperty("name"));
        if ("near".equals(row.getProperty("name")))
          assertThat(((Number) row.getProperty("s")).floatValue()).isCloseTo(1f, within(1e-6f));
      }
    }
    assertThat(names).containsExactlyInAnyOrder("near", "mid");
  }

  @Test
  void aStoredVectorReadsBackAsTheSameFloatsBitForBit() {
    final MutableDocument document = database.newDocument("Special11714");
    document.set("embedding", SPECIAL);
    document.set("empty", new float[0]);
    // newDocument() loaded the remote schema over gRPC, and that response announced the packed kinds: the save sends both
    // arrays packed, so the undeclared properties are stored as the float[] they are
    assertThat(database.sendsPackedArrays()).isTrue();
    document.save();

    final Document reloaded = (Document) database.lookupByRID(document.getIdentity(), true);
    assertRawBits(reloaded.get("embedding"));
    assertThat(reloaded.get("empty")).isEqualTo(new float[0]);

    try (final ResultSet rs = database.query("sql", "SELECT embedding FROM Special11714")) {
      assertRawBits(rs.next().getProperty("embedding"));
    }

    // Inside a client transaction the statement runs on the transaction's own server thread
    database.begin();
    try (final ResultSet rs = database.query("sql", "SELECT embedding FROM Special11714")) {
      assertRawBits(rs.next().getProperty("embedding"));
    } finally {
      database.rollback();
    }
  }

  private static void assertRawBits(final Object value) {
    assertThat(value).isInstanceOf(float[].class);
    final float[] floats = (float[]) value;
    assertThat(floats).hasSameSizeAs(SPECIAL);
    for (int i = 0; i < SPECIAL.length; i++)
      assertThat(Float.floatToRawIntBits(floats[i])).as("element %d", i).isEqualTo(Float.floatToRawIntBits(SPECIAL[i]));
  }
}
