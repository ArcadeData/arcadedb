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
package com.arcadedb.server.http.handler;

import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9913: every Cypher path is a {@code TraversalPath}. Over HTTP it serializes as the
 * {@code {_type: "path", nodes, relationships, length}} object a procedure's path always had - a MATCH path used to be
 * its {@code toString()}, {@code "(#1:0)-[#4:0]->..."} - and Studio's graph view draws its nodes and relationships,
 * which it drew for neither shape.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9913PathOverHttpTest extends BaseGraphServerTest {

  @Test
  void pathsSerializeAsAnObjectAndExpandInTheGraphView() throws Exception {
    executeCommand(0, "sqlscript", "CREATE VERTEX TYPE City; CREATE EDGE TYPE ROAD;"
        + "INSERT INTO City SET name = 'A'; INSERT INTO City SET name = 'B'; INSERT INTO City SET name = 'C';"
        + "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'A') TO (SELECT FROM City WHERE name = 'B') SET km = 1;"
        + "CREATE EDGE ROAD FROM (SELECT FROM City WHERE name = 'B') TO (SELECT FROM City WHERE name = 'C') SET km = 2;");

    for (final String query : new String[] {
        "MATCH (a:City {name: 'A'}), (c:City {name: 'C'}) CALL algo.dijkstra(a, c, 'ROAD', 'km') YIELD path RETURN path",
        "MATCH path = (:City {name: 'A'})-[:ROAD*]->(:City {name: 'C'}) RETURN path",
        "MATCH path = shortestPath((:City {name: 'A'})-[:ROAD*]->(:City {name: 'C'})) RETURN path" }) {
      final JSONObject response = executeCommand(0, "opencypher", query);
      assertThat(response).as(query).isNotNull();
      final JSONObject result = response.getJSONObject("result");

      assertThat(result.getJSONArray("vertices").length()).as(query).isEqualTo(3);
      assertThat(result.getJSONArray("edges").length()).as(query).isEqualTo(2);

      final JSONObject path = result.getJSONArray("records").getJSONObject(0).getJSONObject("path");
      assertThat(path.getString("_type")).as(query).isEqualTo("path");
      assertThat(path.getInt("length")).as(query).isEqualTo(2);
      assertThat(path.getJSONArray("nodes").length()).as(query).isEqualTo(3);
      assertThat(path.getJSONArray("relationships").length()).as(query).isEqualTo(2);
    }
  }
}
