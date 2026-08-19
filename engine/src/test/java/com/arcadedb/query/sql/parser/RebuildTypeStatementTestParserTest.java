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
package com.arcadedb.query.sql.parser;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #6442, item 1: {@code RebuildTypeStatement#copy()} deep-copied every {@code
 * settings} entry's key and value, while the rest of the WITH-settings family ({@code RebuildIndex}/{@code
 * Export}/{@code Backup}/{@code ImportDatabaseStatement}) shallow-copies the same kind of map via {@code
 * settings.putAll(...)}. Normalized on the shallow copy, since {@link Expression} nodes are effectively
 * immutable post-parse and the deep copy had nothing to protect against.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RebuildTypeStatementTestParserTest extends AbstractParserTest {

  @Test
  void plain() {
    checkRightSyntax("REBUILD TYPE Foo");
    checkRightSyntax("rebuild type Foo");
    checkRightSyntax("REBUILD TYPE Foo POLYMORPHIC");
    checkRightSyntax("REBUILD TYPE Foo WITH batchSize = 1000");
    checkRightSyntax("REBUILD TYPE Foo POLYMORPHIC WITH batchSize = 1000, repartition = true");

    checkWrongSyntax("REBUILD TYPE");
  }

  @Test
  void copyAndEqualsKeepTheWithSettings() {
    final RebuildTypeStatement original = (RebuildTypeStatement) checkSyntax(
        "REBUILD TYPE Foo WITH batchSize = 1000, repartition = true", true);

    final RebuildTypeStatement copy = original.copy();
    assertThat(renderSettings(copy)).isEqualTo(renderSettings(original));
    assertThat(copy).isEqualTo(original);
    assertThat(copy.hashCode()).isEqualTo(original.hashCode());

    final RebuildTypeStatement differentSettings = (RebuildTypeStatement) checkSyntax(
        "REBUILD TYPE Foo WITH batchSize = 2000", true);
    assertThat(differentSettings).isNotEqualTo(original);
  }

  /**
   * The fix normalizes on a shallow copy, matching the rest of the family: the copy's settings {@link Map}
   * must be a distinct instance (mutating one must not affect the other), but the {@link Expression} key/value
   * entries it holds are now the SAME instances as the original's, not fresh {@code .copy()} results. Asserting
   * identity here (rather than just equality) is what makes this a regression test for the deep-copy - a
   * revert back to per-entry {@code .copy()} still passes every equals()/hashCode() assertion but fails this one.
   */
  @Test
  void copySettingsMapIsIndependentButSharesTheSameImmutableExpressionInstances() {
    final RebuildTypeStatement original = (RebuildTypeStatement) checkSyntax(
        "REBUILD TYPE Foo WITH batchSize = 1000", true);

    final RebuildTypeStatement copy = original.copy();
    assertThat(copy.settings).isNotSameAs(original.settings);
    assertThat(copy.settings).isEqualTo(original.settings);

    final Map.Entry<Expression, Expression> originalEntry = original.settings.entrySet().iterator().next();
    final Map.Entry<Expression, Expression> copyEntry = copy.settings.entrySet().iterator().next();
    assertThat(copyEntry.getKey()).isSameAs(originalEntry.getKey());
    assertThat(copyEntry.getValue()).isSameAs(originalEntry.getValue());

    copy.settings.clear();
    assertThat(original.settings).isNotEmpty();
  }

  private static Map<String, String> renderSettings(final RebuildTypeStatement statement) {
    final Map<String, String> rendered = new HashMap<>();
    for (final Map.Entry<Expression, Expression> entry : statement.settings.entrySet())
      rendered.put(entry.getKey().toString(), entry.getValue().toString());
    return rendered;
  }
}
