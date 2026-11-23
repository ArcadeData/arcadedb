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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import com.arcadedb.security.SecurityDatabaseUser;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * An unlabelled node pattern reads every vertex type, so a user who may not read one of them is refused by the walk of its buckets
 * whatever the value is. Answering the pattern from the indexes (issue #12522) must not turn that refusal into an answer to "is
 * there a vertex with this value in the type I may not read": the statement is refused the same way for a value the denied type
 * holds, for one it does not, and for one no type holds.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherUnlabelledNodeIndexSeekAclIssue12522Test {
  private static final String PATH = "target/databases/CypherUnlabelledNodeIndexSeekAclIssue12522Test";

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
      // a type with no index, which the walk answers
      database.getSchema().createVertexType("Walked").createProperty("uuid", String.class);
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Open {uuid: 'open-1'})");
      database.command("opencypher", "CREATE (:Secret {uuid: 'secret-1'})");
      database.command("opencypher", "CREATE (:Walked {uuid: 'walked-1'})");
    });
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void aTypeTheUserMayNotReadRefusesTheStatementWhateverTheValueIs() {
    deny("Secret");

    for (final String value : new String[] { "open-1", "secret-1", "walked-1", "nobody" }) {
      final Throwable thrown = catchThrowable(() -> drain("MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", value));
      assertThat(thrown).as("the value %s: a denied type refuses the statement, it does not tell whether it holds the value", value)
          .isInstanceOf(SecurityException.class);
    }
  }

  @Test
  void aTypeWithoutAnIndexTheUserMayNotReadRefusesTheStatementToo() {
    deny("Walked");

    for (final String value : new String[] { "open-1", "walked-1", "nobody" }) {
      final Throwable thrown = catchThrowable(() -> drain("MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", value));
      assertThat(thrown).as("the value %s", value).isInstanceOf(SecurityException.class);
    }
  }

  @Test
  void aWhereFilterIsRefusedTheSameWay() {
    deny("Secret");

    for (final String value : new String[] { "open-1", "nobody" }) {
      final Throwable thrown = catchThrowable(() -> drain("MATCH (n) WHERE n.uuid = $u RETURN n.uuid AS uuid", value));
      assertThat(thrown).as("the value %s", value).isInstanceOf(SecurityException.class);
    }
  }

  @Test
  void aUserWhoMayReadEveryTypeIsAnswered() {
    deny();

    assertThat(drain("MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", "secret-1")).isEqualTo(1);
    assertThat(drain("MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", "nobody")).isZero();
  }

  private int drain(final String cypher, final String value) {
    final Map<String, Object> params = new HashMap<>();
    params.put("u", value);
    int rows = 0;
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
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
