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
package com.arcadedb.query.select;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.LongSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8170
 * <p>
 * The native {@code ilike} folded the case of both operands without a null check, so a single record that does not
 * carry the property aborted the whole query with a {@link NullPointerException}, while {@code like} and SQL
 * {@code ILIKE} simply did not match it. Asserted three ways: against SQL, without an index and with one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8170SelectIlikeMissingPropertyTest extends TestHelper {

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("D");
    type.createProperty("s", Type.STRING);

    database.transaction(() -> {
      final MutableDocument valued = database.newDocument("D");
      valued.set("s", "abc");
      valued.save();

      final MutableDocument absent = database.newDocument("D");
      absent.set("other", 1);
      absent.save();

      final MutableDocument explicitNull = database.newDocument("D");
      explicitNull.set("s", null);
      explicitNull.save();
    });
  }

  @Test
  void ilikeSkipsRecordsWithoutTheProperty() {
    assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("s").ilike().value("A%").count(),
        "s ILIKE 'A%'", 1);
  }

  @Test
  void likeStillSkipsRecordsWithoutTheProperty() {
    assertSameWithAndWithoutIndex(() -> database.select().fromType("D").where().property("s").like().value("a%").count(),
        "s LIKE 'a%'", 1);
  }

  @Test
  void ilikeWithANullPatternNeverMatches() {
    assertThat(database.select().fromType("D").where().property("s").ilike().value(null).count()).isZero();
  }

  @Test
  void nonStringAndMultiValueOperandsMatchLikeSql() {
    // THE NATIVE OPERATORS NOW EVALUATE THROUGH THE SQL ONES: A NON-STRING VALUE IS MATCHED BY ITS STRING FORM INSTEAD
    // OF FAILING A CAST, AND A LIST MATCHES WHEN ANY OF ITS ITEMS DOES
    database.getSchema().createDocumentType("M");
    database.transaction(() -> {
      database.newDocument("M").set("n", 123).set("tags", List.of("Alpha", "beta")).save();
      database.newDocument("M").set("n", 456).set("tags", List.of("gamma")).save();
    });

    assertThat(database.select().fromType("M").where().property("n").like().value("12%").count()).isEqualTo(
        sqlCount("M", "n LIKE '12%'")).isEqualTo(1);
    assertThat(database.select().fromType("M").where().property("n").ilike().value("45%").count()).isEqualTo(
        sqlCount("M", "n ILIKE '45%'")).isEqualTo(1);
    assertThat(database.select().fromType("M").where().property("tags").ilike().value("ALPHA").count()).isEqualTo(
        sqlCount("M", "tags ILIKE 'ALPHA'")).isEqualTo(1);
    assertThat(database.select().fromType("M").where().property("tags").like().value("gam%").count()).isEqualTo(
        sqlCount("M", "tags LIKE 'gam%'")).isEqualTo(1);
  }

  private void assertSameWithAndWithoutIndex(final LongSupplier nativeCount, final String sqlWhere, final long expected) {
    assertThat(sqlCount(sqlWhere)).as("SQL baseline for " + sqlWhere).isEqualTo(expected);
    assertThat(nativeCount.getAsLong()).as("native without an index: " + sqlWhere).isEqualTo(expected);

    database.getSchema().getType("D").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "s");
    try {
      assertThat(nativeCount.getAsLong()).as("native with an index: " + sqlWhere).isEqualTo(expected);
    } finally {
      for (final var index : database.getSchema().getType("D").getIndexesByProperties("s"))
        database.getSchema().dropIndex(index.getName());
    }
  }

  private long sqlCount(final String where) {
    return sqlCount("D", where);
  }

  private long sqlCount(final String type, final String where) {
    return ((Number) database.query("sql", "SELECT count(*) AS c FROM " + type + " WHERE " + where).nextIfAvailable()
        .getProperty("c")).longValue();
  }
}
