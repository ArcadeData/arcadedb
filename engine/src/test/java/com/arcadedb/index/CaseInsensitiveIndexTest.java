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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests case-insensitive (CI) collation on LSM tree indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CaseInsensitiveIndexTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Product")
          .createProperty("Name", Type.STRING);
      database.getSchema().getType("Product").createProperty("Code", Type.STRING);

      database.command("sql", "CREATE INDEX ON Product (Name COLLATE CI) NOTUNIQUE");
    });

    database.transaction(() -> {
      database.newDocument("Product").set("Name", "Hello World").set("Code", "HW1").save();
      database.newDocument("Product").set("Name", "HELLO WORLD").set("Code", "HW2").save();
      database.newDocument("Product").set("Name", "hello world").set("Code", "HW3").save();
      database.newDocument("Product").set("Name", "ArcadeDB").set("Code", "ADB").save();
    });
  }

  @Test
  void cILookupWithExactCase() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Product WHERE Name = 'Hello World'");
      // CI index: all 3 case variants of "hello world" should match
      assertThat(rs.stream().count()).isEqualTo(3);
    });
  }

  @Test
  void cILookupWithDifferentCase() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Product WHERE Name = 'HELLO WORLD'");
      assertThat(rs.stream().count()).isEqualTo(3);
    });
  }

  @Test
  void cILookupWithToLowerCase() {
    // This is the main use case: Name.toLowerCase() = 'value' should use the CI index
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM Product WHERE Name.toLowerCase() = 'hello world'");
      assertThat(rs.stream().count()).isEqualTo(3);
    });
  }

  @Test
  void cIIndexUsedInExecutionPlan() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "SELECT FROM Product WHERE Name.toLowerCase() = 'hello world'");
      final String plan = rs.getExecutionPlan().get().prettyPrint(0, 2);
      assertThat(plan).contains("FETCH FROM INDEX");
      rs.stream().count(); // consume
    });
  }

  @Test
  void cINonMatchingValue() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Product WHERE Name = 'nonexistent'");
      assertThat(rs.stream().count()).isEqualTo(0);
    });
  }

  @Test
  void cILookupDistinctValue() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Product WHERE Name = 'arcadedb'");
      assertThat(rs.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void cIPersistenceAcrossReopen() {
    database.close();
    database = factory.open();

    database.transaction(() -> {
      // After reopen, CI index should still work
      final ResultSet rs = database.query("sql", "SELECT FROM Product WHERE Name = 'ARCADEDB'");
      assertThat(rs.stream().count()).isEqualTo(1);
    });
  }

  @Test
  void cIUniqueIndex() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("UniqueCI")
          .createProperty("Code", Type.STRING);
      database.command("sql", "CREATE INDEX ON UniqueCI (Code COLLATE CI) UNIQUE");
    });

    database.transaction(() -> {
      database.newDocument("UniqueCI").set("Code", "ABC").save();
    });

    // Inserting same value with different case should fail
    assertThatThrownBy(() -> database.transaction(() ->
        database.newDocument("UniqueCI").set("Code", "abc").save()
    )).isInstanceOf(Exception.class);
  }

  @Test
  void compositeIndexPartialCI() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Composite")
          .createProperty("Name", Type.STRING);
      database.getSchema().getType("Composite").createProperty("Code", Type.STRING);
      // CI on Name only, default on Code
      database.command("sql", "CREATE INDEX ON Composite (Name COLLATE CI, Code) UNIQUE");
    });

    database.transaction(() -> {
      database.newDocument("Composite").set("Name", "Foo").set("Code", "A").save();
      database.newDocument("Composite").set("Name", "FOO").set("Code", "B").save(); // same name different code: OK
    });

    database.transaction(() -> {
      // Name is CI, so "foo" matches both
      final ResultSet rs = database.query("sql", "SELECT FROM Composite WHERE Name = 'foo'");
      assertThat(rs.stream().count()).isEqualTo(2);
    });
  }
}
