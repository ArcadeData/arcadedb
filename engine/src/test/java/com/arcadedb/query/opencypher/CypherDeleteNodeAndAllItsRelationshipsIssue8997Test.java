/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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

import com.arcadedb.TestHelper;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code MATCH (n)-[r]-() DELETE r, n} deletes a node bound to several relationships: the DeleteConnectedNode check runs once
 * every row is processed, not per row (#8997).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDeleteNodeAndAllItsRelationshipsIssue8997Test extends TestHelper {

  @BeforeEach
  void load() {
    database.transaction(() -> database.command("opencypher", """
        CREATE (n:N {id: 1}), (n)-[:R]->(:N {id: 2}), (n)-[:R]->(:N {id: 3}),
               (m:N {id: 4}), (m)-[:R]->(:N {id: 5})""").close());
  }

  private List<String> remaining() {
    final List<String> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher",
        "MATCH (n:N) OPTIONAL MATCH (n)-[r]->() RETURN n.id AS id, count(r) AS out ORDER BY id")) {
      while (rs.hasNext()) {
        final var r = rs.next();
        out.add(r.<Object>getProperty("id") + ":" + r.<Object>getProperty("out"));
      }
    }
    return out;
  }

  @Test
  void deleteRelationshipsAndNodeWithTwoRelationships() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 4})-[r]-() DELETE r, n").close());
    database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 1})-[r]-() DELETE r, n").close());
    assertThat(remaining()).containsExactly("2:0", "3:0", "5:0");
  }

  @Test
  void deleteWithOptionalMatch() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 1}) OPTIONAL MATCH (n)-[r]-() DELETE n, r").close());
    assertThat(remaining()).containsExactly("2:0", "3:0", "4:1", "5:0");
  }

  @Test
  void nodeStillConnectedAfterEveryRowStillFails() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (n:N {id: 1})-[r]-() DELETE n").close()))
        .hasMessageContaining("DeleteConnectedNode")
        .isInstanceOf(CommandExecutionException.class);
    assertThat(remaining()).containsExactly("1:2", "2:0", "3:0", "4:1", "5:0");
  }
}
