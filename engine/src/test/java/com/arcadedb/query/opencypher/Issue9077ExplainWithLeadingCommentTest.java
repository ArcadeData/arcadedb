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

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9077: {@code EXPLAIN} and {@code PROFILE} were not recognised after a leading comment.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9077ExplainWithLeadingCommentTest extends TestHelper {
  @Test
  void explainAndProfileAcceptLeadingComments() {
    database.getSchema().createVertexType("Person");
    database.transaction(() -> database.command("opencypher", "CREATE (:Person {name: 'a'})").close());

    for (final String prefix : new String[] { "// c\n", "/* c */ ", "/* a */\n// b\n  ", "" }) {
      try (final ResultSet rs = database.command("opencypher", prefix + "EXPLAIN MATCH (p:Person) RETURN p.name")) {
        final Object plan = rs.next().getProperty("executionPlan");
        assertThat(plan).as(prefix).isNotNull();
      }
      try (final ResultSet rs = database.command("opencypher", prefix + "profile MATCH (p:Person) RETURN p.name")) {
        final String name = rs.next().getProperty("p.name");
        assertThat(name).as(prefix).isEqualTo("a");
      }
      try (final ResultSet rs = database.query("opencypher", prefix + "EXPLAIN MATCH (p:Person) RETURN p.name")) {
        final Object plan = rs.next().getProperty("executionPlan");
        assertThat(plan).as(prefix).isNotNull();
      }
    }
  }
}
