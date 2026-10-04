/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
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
package com.arcadedb.schema;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A scalar written to a LIST OF &lt;number&gt; property is wrapped as one element and converted like the elements of a list are (#9028).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class Issue9028ScalarIntoTypedListTest extends TestHelper {

  @Test
  void sqlScalarIsConvertedToTheDeclaredElementType() {
    database.command("sql", "CREATE DOCUMENT TYPE T");
    database.command("sql", "CREATE PROPERTY T.anything LIST");
    database.command("sql", "CREATE PROPERTY T.longs LIST OF LONG");
    database.command("sql", "CREATE PROPERTY T.doubles LIST OF DOUBLE");

    database.transaction(() -> {
      final Result r = database.command("sql", "INSERT INTO T SET anything = 5, longs = 5, doubles = 5").next();
      assertThat((List<Object>) r.getProperty("anything")).containsExactly(5);
      assertThat((List<Object>) r.getProperty("longs")).containsExactly(5L);
      assertThat((List<Object>) r.getProperty("doubles")).containsExactly(5.0D);
    });
    database.transaction(() -> {
      final Result r = database.command("sql", "INSERT INTO T SET longs = [5], doubles = 5.0").next();
      assertThat((List<Object>) r.getProperty("longs")).containsExactly(5L);
      assertThat((List<Object>) r.getProperty("doubles")).containsExactly(5.0D);
    });
  }

  @Test
  void cypherCreateAndSetAgreeOnScalarIntoTypedList() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.ints LIST OF INTEGER");
    database.command("sql", "CREATE PROPERTY V.longs LIST OF LONG");

    database.transaction(() -> database.command("opencypher", "CREATE (:V {id: 0})").close());
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:V {id: 1, ints: 5, longs: 5})").close();
      database.command("opencypher", "MATCH (n:V {id: 0}) SET n.ints = 5, n.longs = 5").close();
    });

    for (final int id : new int[] { 0, 1 }) {
      final Result r = database.query("sql", "SELECT ints, longs FROM V WHERE id = " + id).next();
      assertThat((List<Object>) r.getProperty("ints")).containsExactly(5);
      assertThat((List<Object>) r.getProperty("longs")).containsExactly(5L);
    }
  }

  @Test
  void primitiveArrayAndOtherElementTypes() {
    database.command("sql", "CREATE DOCUMENT TYPE P");
    database.command("sql", "CREATE PROPERTY P.longs LIST OF LONG");
    database.command("sql", "CREATE PROPERTY P.strs LIST OF STRING");
    database.command("sql", "CREATE PROPERTY P.ints LIST OF INTEGER");
    database.transaction(() -> {
      final var doc = database.newDocument("P");
      doc.set("longs", new int[] { 1, 2 });
      doc.set("strs", 7);
      doc.save();
      assertThat((List<Object>) doc.get("longs")).containsExactly(1L, 2L);
      assertThat((List<Object>) doc.get("strs")).containsExactly("7");
    });
    org.assertj.core.api.Assertions.assertThatThrownBy(
        () -> database.transaction(() -> database.command("sql", "INSERT INTO P SET ints = 'abc'"))).isNotNull();
  }
}
