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

import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issue #7792: {@code CreateIndexStatement.ifNotExists} was read by {@code executeDDL} and fed
 * to the index builder, but {@code toString()} never emitted the clause and {@code getIdentityElements()} never
 * included the field, so a re-rendered {@code CREATE INDEX IF NOT EXISTS} came back as the strict form (which
 * throws when the index already exists instead of being a no-op) and compared equal to it. {@code copy()} dropped
 * the field too.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7792CreateIndexIfNotExistsTest extends AbstractParserTest {

  @Test
  void reRendersIfNotExistsWithoutAName() {
    final Statement result = (Statement) checkRightSyntax("CREATE INDEX IF NOT EXISTS ON Foo (a) UNIQUE");
    assertThat(result.toString()).contains("IF NOT EXISTS");
  }

  @Test
  void reRendersIfNotExistsWithAName() {
    final Statement result = (Statement) checkRightSyntax("CREATE INDEX idx2 IF NOT EXISTS ON Foo (a) NOTUNIQUE");
    assertThat(result.toString()).isEqualTo("CREATE INDEX idx2 IF NOT EXISTS ON Foo (a) NOTUNIQUE NULL_STRATEGY SKIP");
  }

  @Test
  void ifNotExistsFormNoLongerComparesEqualToThePlainForm() {
    final CreateIndexStatement withIfNotExists = (CreateIndexStatement) new SQLAntlrParser(null)
        .parse("CREATE INDEX IF NOT EXISTS ON Foo (a) UNIQUE");
    final CreateIndexStatement plain = (CreateIndexStatement) new SQLAntlrParser(null)
        .parse("CREATE INDEX ON Foo (a) UNIQUE");

    assertThat(withIfNotExists).isNotEqualTo(plain);
  }

  @Test
  void copyPreservesIfNotExists() {
    final CreateIndexStatement stmt = (CreateIndexStatement) new SQLAntlrParser(null)
        .parse("CREATE INDEX idx2 IF NOT EXISTS ON Foo (a) NOTUNIQUE");
    final CreateIndexStatement copy = stmt.copy();

    assertThat(copy.ifNotExists).isTrue();
    assertThat(copy).isEqualTo(stmt);
    assertThat(copy.toString()).isEqualTo(stmt.toString());
  }
}
