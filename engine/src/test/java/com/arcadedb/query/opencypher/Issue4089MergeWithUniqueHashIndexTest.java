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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for GitHub issue #4089.
 * <p>
 * MERGE on a node whose match property is backed by a UNIQUE HASH index must
 * find the existing vertex on the second invocation; otherwise the engine
 * tries to create a duplicate and throws DuplicatedKeyException.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue4089MergeWithUniqueHashIndexTest {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/issue-4089-merge-unique-hash").create();
    database.transaction(() -> {
      final VertexType type = database.getSchema().createVertexType("Experiment");
      type.createProperty("pk", Type.STRING).setMandatory(true).setNotNull(true);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.HASH, true, "Experiment", "pk");
    });
  }

  @AfterEach
  void cleanup() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeFindsExistingVertexThroughUniqueHashIndex() {
    final String query = "MERGE (v:Experiment {pk: 'a'}) "
        + "ON CREATE SET v.status = 'created' "
        + "ON MATCH SET v.status = 'updated' "
        + "RETURN v";

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", query);
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("v.status")).isIn("created", null);
    });

    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", query);
      assertThat(rs.hasNext()).isTrue();
      rs.next();
    });

    final ResultSet verify = database.query("opencypher", "MATCH (v:Experiment {pk: 'a'}) RETURN v.status AS status");
    int count = 0;
    String finalStatus = null;
    while (verify.hasNext()) {
      finalStatus = verify.next().<String>getProperty("status");
      count++;
    }
    assertThat(count).isEqualTo(1);
    assertThat(finalStatus).isEqualTo("updated");
  }

  @Test
  void mergeFindsExistingVertexThroughUniqueLsmTreeIndex() {
    database.transaction(() -> {
      database.getSchema().getType("Experiment").getIndexesByProperties("pk")
          .forEach(idx -> database.getSchema().dropIndex(idx.getName()));
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Experiment", "pk");
    });

    final String query = "MERGE (v:Experiment {pk: 'b'}) "
        + "ON CREATE SET v.status = 'created' "
        + "ON MATCH SET v.status = 'updated' "
        + "RETURN v";

    database.transaction(() -> database.command("opencypher", query));
    database.transaction(() -> database.command("opencypher", query));

    final ResultSet verify = database.query("opencypher",
        "MATCH (v:Experiment {pk: 'b'}) RETURN v.status AS status");
    int count = 0;
    String finalStatus = null;
    while (verify.hasNext()) {
      finalStatus = verify.next().<String>getProperty("status");
      count++;
    }
    assertThat(count).isEqualTo(1);
    assertThat(finalStatus).isEqualTo("updated");
  }
}
