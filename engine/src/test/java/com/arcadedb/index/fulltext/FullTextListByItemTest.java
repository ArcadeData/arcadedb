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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test for GitHub Issue #2693: BY ITEM FULL_TEXT index support.
 * Tests that all search operators work correctly with FULL_TEXT indexes on LIST properties using BY ITEM syntax.
 */
public class FullTextListByItemTest extends TestHelper {

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("doc");
      database.getSchema().getType("doc").createProperty("txt", Type.LIST);
    });
  }

  @Test
  void fullTextByItemContainsText() {
    // Test CONTAINSTEXT operator with FULL_TEXT BY ITEM index
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt CONTAINSTEXT 'two'");
      assertThat(result.hasNext()).as("CONTAINSTEXT should find 'two'").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemMatches() {
    // Test MATCHES operator with FULL_TEXT BY ITEM index
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt MATCHES 'two'");
      assertThat(result.hasNext()).as("MATCHES should find 'two'").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemMatchesCaseInsensitive() {
    // Test MATCHES operator with case-insensitive regex
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt MATCHES '(?i)two'");
      assertThat(result.hasNext()).as("MATCHES with (?i) should find 'two'").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemLike() {
    // Test LIKE operator with FULL_TEXT BY ITEM index - Issue #2693
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt LIKE '%two%'");
      assertThat(result.hasNext()).as("LIKE should find 'two'").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemILike() {
    // Test ILIKE operator (case-insensitive LIKE) with FULL_TEXT BY ITEM index - Issue #2693
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt ILIKE '%TWO%'");
      assertThat(result.hasNext()).as("ILIKE should find 'two' (case insensitive)").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemFuzzyMatch() {
    // Test fuzzy matching operator (<=>) with FULL_TEXT BY ITEM index - Issue #2693
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt <=> 'two'");
      assertThat(result.hasNext()).as("Fuzzy match (<=>)  should find 'two'").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemEquality() {
    // Test equality operator with FULL_TEXT BY ITEM index (should work)
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['four','five','six']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt = 'two'");
      assertThat(result.hasNext()).as("Equality operator should find 'two'").isTrue();
      assertThat(result.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void fullTextByItemMultipleMatches() {
    // Test that multiple documents can match the same search term
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['one','two','three']");
      database.command("sql", "INSERT INTO doc SET txt = ['two','four','five']");
      database.command("sql", "INSERT INTO doc SET txt = ['six','seven','eight']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt CONTAINSTEXT 'two'");
      assertThat(result.stream().count()).as("Should find 2 documents containing 'two'").isEqualTo(2);
    });
  }

  @Test
  void fullTextByItemPartialMatch() {
    // Test partial word matching with wildcards
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON doc (txt BY ITEM) FULL_TEXT");
    });

    database.transaction(() -> {
      database.command("sql", "INSERT INTO doc SET txt = ['database','python','javascript']");
      database.command("sql", "INSERT INTO doc SET txt = ['typescript','golang','rust']");
    });

    database.transaction(() -> {
      ResultSet result = database.query("sql", "SELECT FROM doc WHERE txt LIKE '%script%'");
      assertThat(result.stream().count()).as("Should find documents with 'script' substring").isEqualTo(2);
    });
  }
}
