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
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #10138: {@code path.elements(null)} answered an empty list, where every other path function
 * answers null for a null argument.
 */
class Issue10138PathElementsNullTest extends TestHelper {

  @Test
  void nullArgumentAnswersNull() {
    final Result row = single("RETURN path.elements(null) AS r");
    assertThat(row.hasProperty("r")).isTrue();
    assertThat((Object) row.getProperty("r")).isNull();
  }

  @Test
  void agreesWithTheOtherPathFunctions() {
    final Result row = single("""
        RETURN path.elements(null) IS NULL AS elements, nodes(null) IS NULL AS nodes,
               relationships(null) IS NULL AS rels, size(path.elements(null)) IS NULL AS size""");
    assertThat(row.<Boolean>getProperty("elements")).isTrue();
    assertThat(row.<Boolean>getProperty("nodes")).isTrue();
    assertThat(row.<Boolean>getProperty("rels")).isTrue();
    assertThat(row.<Boolean>getProperty("size")).isTrue();
  }

  @Test
  void aMatchedPathStillAnswersItsElements() {
    database.transaction(() -> database.command("opencypher", "CREATE (:N {id: 1})-[:R]->(:N {id: 2})"));
    final Result row = single("MATCH p = (:N {id: 1})-[:R]->(:N {id: 2}) RETURN size(path.elements(p)) AS n");
    assertThat(row.<Number>getProperty("n").intValue()).isEqualTo(3);
  }

  @Test
  void aNonPathArgumentIsStillRefused() {
    assertThatThrownBy(() -> single("RETURN path.elements(1) AS r"))
        .isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("PATH");
  }

  private Result single(final String query) {
    try (final ResultSet rs = database.query("opencypher", query)) {
      return rs.next();
    }
  }
}
