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

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class MatchResultTest extends TestHelper {

  /**
   * MATCH query with NOT pattern fails to return unique vertices
   * https://github.com/ArcadeData/arcadedb/issues/1689
   */
  @Test
  public void testIssue1689() {
    database.transaction(() -> database.command("sqlscript", """
        CREATE VERTEX TYPE Person IF NOT EXISTS;
        CREATE PROPERTY Person.role IF NOT EXISTS STRING;
        CREATE VERTEX TYPE House IF NOT EXISTS;
        CREATE EDGE TYPE LivesIn IF NOT EXISTS;
        CREATE VERTEX House;
        CREATE VERTEX Person SET role='mom';
        CREATE VERTEX Person SET role='dad';
        CREATE VERTEX Person SET role='child';
        CREATE EDGE LivesIn FROM (SELECT FROM Person) TO (SELECT FROM House);
        """));

    final ResultSet resultSet = database.query("sql", """
        MATCH {TYPE: Person, AS: personVertex} -LivesIn-> {TYPE: House}
        , NOT {AS: personVertex} -DummyEdge-> {TYPE: House}
        RETURN personVertex
        """);
    Set<RID> set = new HashSet<>();
    while (resultSet.hasNext()) {
      final Vertex next = resultSet.nextIfAvailable().getProperty("personVertex");
      set.add(next.getIdentity());
    }

    assertThat(set.size()).isEqualTo(3);
  }
}
