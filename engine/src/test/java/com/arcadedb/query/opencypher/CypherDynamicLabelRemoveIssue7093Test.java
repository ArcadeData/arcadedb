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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7093: {@code REMOVE n:$('V')} succeeded without an error but left label {@code V} on the node, while the
 * static {@code REMOVE n:V} removed it. The reporter's exact script: an unlabelled {@code MATCH} on a property, a
 * literal-string dynamic label, and a node created with two labels in one pattern. The dynamic-label interpolation
 * landed with issue #7059; this pins the reporter's scenario, which differs from the #7059 coverage in every one of
 * those three respects, so a regression in any of them is caught here.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherDynamicLabelRemoveIssue7093Test extends TestHelper {

  private List<String> labelsOfNode1() {
    try (final ResultSet rs = database.query("opencypher", "MATCH (n {id: 1}) RETURN labels(n) AS l")) {
      final List<String> labels = rs.next().getProperty("l");
      assertThat(rs.hasNext()).isFalse();
      return labels;
    }
  }

  @Test
  void aLiteralStringDynamicLabelIsRemovedLikeTheStaticForm() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:U:V {id: 1})").close());
    assertThat(labelsOfNode1()).containsExactlyInAnyOrder("U", "V");

    database.transaction(() -> database.command("opencypher", "MATCH (n {id: 1}) REMOVE n:$('V')").close());

    assertThat(labelsOfNode1()).containsExactly("U");
  }

  @Test
  void theStaticFormStaysTheControl() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:U:V {id: 1})").close());

    database.transaction(() -> database.command("opencypher", "MATCH (n {id: 1}) REMOVE n:V").close());

    assertThat(labelsOfNode1()).containsExactly("U");
  }

  /** Both spellings in one statement, each removing one of the two labels, leave the node with neither. */
  @Test
  void staticAndDynamicRemovalsCombineInOneClause() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:U:V:W {id: 1})").close());

    database.transaction(() -> database.command("opencypher", "MATCH (n {id: 1}) REMOVE n:U:$('V')").close());

    assertThat(labelsOfNode1()).containsExactly("W");
  }

  /** The {@code IS} spelling of the same removal. */
  @Test
  void theIsSpellingRemovesTheDynamicLabelToo() {
    database.transaction(() -> database.command("opencypher", "CREATE (n:U:V {id: 1})").close());

    database.transaction(() -> database.command("opencypher", "MATCH (n {id: 1}) REMOVE n IS $('V')").close());

    assertThat(labelsOfNode1()).containsExactly("U");
  }
}
