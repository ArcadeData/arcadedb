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
 * removed, while the unquoted {@code REMOVE n.p} removed it. A backtick-quoted name is the same name as the bare one,
 * and it is the only way to write a name that holds a dot, so those are pinned here as well: a variable or a property
 * key such as {@code `a.b`} must not be split in the middle.
 */
class CypherRemoveBacktickedPropertyIssue11839Test extends TestHelper {

  /** The value {@code expression} answers for the one {@code RemoveBacktickRepro} node the test created. */
  private Object valueOf(final String expression) {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:RemoveBacktickRepro) RETURN " + expression + " AS v")) {
      final Object value = rs.next().getProperty("v");
      assertThat(rs.hasNext()).isFalse();
      return value;
    }
  }

  private void create(final String properties) {
    database.transaction(() -> database.command("opencypher", "CREATE (n:RemoveBacktickRepro " + properties + ")").close());
  }

  private void remove(final String query) {
    database.transaction(() -> database.command("opencypher", query).close());
  }

  @Test
  void backtickedPropertyIsRemoved() {
    create("{p: 123, q: 7}");
    assertThat(valueOf("n.p")).isEqualTo(123);

    remove("MATCH (n:RemoveBacktickRepro) REMOVE n.`p`");

    assertThat(valueOf("n.p")).isNull();
    assertThat(valueOf("n.q")).as("only the named property goes").isEqualTo(7);
  }

  @Test
  void backtickedVariableIsRemoved() {
    create("{p: 123, q: 7}");

    remove("MATCH (`n`:RemoveBacktickRepro) REMOVE `n`.p");

    assertThat(valueOf("n.p")).isNull();
    assertThat(valueOf("n.q")).isEqualTo(7);
  }

  @Test
  void backtickedVariableAndPropertyAreRemoved() {
    create("{p: 123, q: 7}");

    remove("MATCH (`n`:RemoveBacktickRepro) REMOVE `n`.`p`");

    assertThat(valueOf("n.p")).isNull();
    assertThat(valueOf("n.q")).isEqualTo(7);
  }

  @Test
  void backtickedVariableWithDynamicKeyIsRemoved() {
    create("{p: 123, q: 7}");

    remove("MATCH (`n`:RemoveBacktickRepro) REMOVE `n`['p']");

    assertThat(valueOf("n.p")).isNull();
    assertThat(valueOf("n.q")).isEqualTo(7);
  }

  @Test
  void variableHoldingADotIsRemoved() {
    create("{p: 123, q: 7}");

    remove("MATCH (`a.b`:RemoveBacktickRepro) REMOVE `a.b`.p");

    assertThat(valueOf("n.p")).isNull();
    assertThat(valueOf("n.q")).isEqualTo(7);
  }

  @Test
  void propertyKeyHoldingADotIsRemoved() {
    create("{p: 123, `x.y`: 5}");
    assertThat(valueOf("n.`x.y`")).isEqualTo(5);

    remove("MATCH (n:RemoveBacktickRepro) REMOVE n.`x.y`");

    assertThat(valueOf("n.`x.y`")).isNull();
    assertThat(valueOf("n.p")).as("the neighbour of the dotted key stays").isEqualTo(123);
  }

  @Test
  void unquotedFormStaysTheControl() {
    create("{p: 123, q: 7}");

    remove("MATCH (n:RemoveBacktickRepro) REMOVE n.p");

    assertThat(valueOf("n.p")).isNull();
    assertThat(valueOf("n.q")).isEqualTo(7);
  }
}
