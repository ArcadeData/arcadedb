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
package com.arcadedb.gremlin.integration.importer;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.integration.importer.Importer;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URL;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #6751. On the GraphSON path used for non-RID vertex ids, a multi-cardinality (list/set) vertex property was
 * truncated to element [0] and every further value was dropped without a warning.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6751GraphSONMultiPropertyTest {

  private static final String DATABASE_PATH = "target/databases/issue-6751-graphson-multi-property";

  @BeforeEach
  @AfterEach
  void cleanUp() {
    FileUtils.deleteRecursively(new File(DATABASE_PATH));
  }

  @Test
  void everyValueOfAMultiCardinalityPropertyIsImported() {
    final URL inputFile = Issue6751GraphSONMultiPropertyTest.class.getClassLoader()
        .getResource("graphson-multi-property.graphson");
    assertThat(inputFile).isNotNull();

    final Database database = new DatabaseFactory(DATABASE_PATH).create();
    try {
      new Importer(database, inputFile.getFile()).load();

      final Document n1 = lookupByOriginalId(database, "http://ex/n1");
      final Document n2 = lookupByOriginalId(database, "http://ex/n2");

      // THE THREE VALUES OF THE LIST-CARDINALITY PROPERTY, INCLUDING THE GRAPHSON-TYPED ONE.
      assertThat((Iterable<Object>) n1.get("skill")).containsExactly("java", "sql", "gremlin");

      // TYPED VALUES KEEP THEIR TYPE WHEN THEY ARE PART OF A MULTI-VALUE PROPERTY.
      assertThat((Iterable<Object>) n2.get("score")).containsExactly(1, 2);

      // A SINGLE-ENTRY PROPERTY STAYS SCALAR: THE FIX MUST NOT WIDEN EVERY PROPERTY INTO A LIST.
      assertThat(n1.get("name")).isEqualTo("Marko");
      assertThat(n2.get("name")).isEqualTo("Vadas");
    } finally {
      database.drop();
    }
  }

  private static Document lookupByOriginalId(final Database database, final String originalId) {
    try (final ResultSet result = database.query("sql", "select from person where `@id` = ?", originalId)) {
      assertThat(result.hasNext()).isTrue();
      return result.next().getElement().orElseThrow();
    }
  }
}
