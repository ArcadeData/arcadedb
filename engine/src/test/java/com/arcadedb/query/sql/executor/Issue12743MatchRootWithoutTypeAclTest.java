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
package com.arcadedb.query.sql.executor;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The untyped SQL MATCH root of #12743 walks the buckets of every vertex type, so it must read them through the per-bucket gate,
 * as an unlabelled openCypher {@code MATCH (n)} and a {@code SELECT FROM V} do: a user who may not read one vertex type is
 * refused the statement whatever the value its {@code where:} asks for, so the refusal cannot tell whether the denied type holds
 * it, and the answer is never a silent subset that would differ from the openCypher one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue12743MatchRootWithoutTypeAclTest {
  private static final String PATH = "target/databases/Issue12743MatchRootWithoutTypeAclTest";

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();

    database.transaction(() -> {
      for (final String name : new String[] { "Open", "Secret" }) {
        final VertexType type = database.getSchema().createVertexType(name);
        type.createProperty("uuid", String.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      }
      database.command("opencypher", "CREATE (:Open {uuid: 'open-1'}), (:Secret {uuid: 'secret-1'})");
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aVertexTypeTheUserMayNotReadRefusesTheUntypedRoot() {
    deny("Secret");

    assertThat(catchThrowable(() -> drain("sql", "MATCH {as: n} RETURN n.uuid AS uuid", null))).isInstanceOf(SecurityException.class);
    for (final String value : new String[] { "open-1", "secret-1", "nobody" }) {
      assertThat(catchThrowable(() -> drain("sql", "MATCH {as: n, where: (uuid = :u)} RETURN n.uuid AS uuid", value)))
          .as("the value %s: a denied type refuses the statement, it does not tell whether it holds the value", value)
          .isInstanceOf(SecurityException.class);
      // THE SAME ANSWER AS THE UNLABELLED openCypher NODE
      assertThat(catchThrowable(() -> drain("opencypher", "MATCH (n) WHERE n.uuid = $u RETURN n.uuid AS uuid", value)))
          .as("the value %s", value).isInstanceOf(SecurityException.class);
    }
  }

  @Test
  void aUserWhoMayReadEveryVertexTypeIsAnswered() {
    deny();

    assertThat(drain("sql", "MATCH {as: n} RETURN n.uuid AS uuid", null)).isEqualTo(2);
    assertThat(drain("sql", "MATCH {as: n, where: (uuid = :u)} RETURN n.uuid AS uuid", "secret-1")).isEqualTo(1);
    assertThat(drain("sql", "MATCH {as: n, where: (uuid = :u)} RETURN n.uuid AS uuid", "nobody")).isZero();
  }

  private int drain(final String language, final String query, final String value) {
    final Map<String, Object> params = new HashMap<>();
    if (value != null)
      params.put("u", value);
    int rows = 0;
    try (final ResultSet rs = database.query(language, query, params)) {
      while (rs.hasNext()) {
        rs.next();
        rows++;
      }
    }
    return rows;
  }

  /** Denies the read of every record of the types, and grants everything else. */
  private void deny(final String... typeNames) {
    final Set<Integer> deniedBucketIds = new HashSet<>();
    for (final String typeName : typeNames)
      deniedBucketIds.addAll(database.getSchema().getType(typeName).getBucketIds(false));

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "restricted";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return !deniedBucketIds.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
