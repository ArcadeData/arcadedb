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
 * Issue #11839: {@code REMOVE n.`p`} kept the backticks in the variable and property names, so the property was not
 * removed, while the unquoted {@code REMOVE n.p} removed it.
 */
class CypherRemoveBacktickedPropertyIssue11839Test extends TestHelper {

  private Object propertyP() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:RemoveBacktickRepro) RETURN n.p AS p")) {
      final Object value = rs.next().getProperty("p");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }

  @Test
  void backtickedPropertyIsRemoved() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:RemoveBacktickRepro {p: 123})").close());
    assertThat(propertyP()).isEqualTo(123);

    database.transaction(
        () -> database.command("opencypher", "MATCH (n:RemoveBacktickRepro) REMOVE n.`p`").close());

    assertThat(propertyP()).isNull();
  }

  @Test
  void backtickedVariableIsRemoved() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:RemoveBacktickRepro {p: 123})").close());

    database.transaction(
        () -> database.command("opencypher", "MATCH (`n`:RemoveBacktickRepro) REMOVE `n`.p").close());

    assertThat(propertyP()).isNull();
  }

  @Test
  void unquotedFormStaysTheControl() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:RemoveBacktickRepro {p: 123})").close());

    database.transaction(() -> database.command("opencypher", "MATCH (n:RemoveBacktickRepro) REMOVE n.p").close());

    assertThat(propertyP()).isNull();
  }
}
