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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression tests for issues #4879, #4880, #4881: CREATE INDEX on MAP properties with
 * {@code BY KEY} / {@code BY VALUE}, index-name uniqueness across modifiers, and the
 * {@code IF NOT EXISTS} + {@code BY ITEM/KEY/VALUE} created-flag behaviour.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class MapIndexByKeyValueTest extends TestHelper {

  @Override
  public void beginTest() {
    // Reproduce the exact DDL used in the issue reports (#4879/#4880): a MAP property declared via SQL
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Movie");
      database.command("sql", "CREATE PROPERTY Movie.title STRING");
      database.command("sql", "CREATE PROPERTY Movie.thumbs MAP OF STRING");
    });

    assertThat(database.getSchema().getType("Movie").getProperty("thumbs").getType()).isEqualTo(Type.MAP);
  }

  /**
   * Issue #4880: CREATE INDEX ... BY KEY / BY VALUE must be created without failing with
   * "property does not exist".
   */
  @Test
  void createIndexByKeyAndByValue() {
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Movie (thumbs BY KEY) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Movie (thumbs BY VALUE) NOTUNIQUE");
    });

    assertThat(database.getSchema().existsIndex("Movie[thumbsbykey]")).isTrue();
    assertThat(database.getSchema().existsIndex("Movie[thumbsbyvalue]")).isTrue();
  }

  /**
   * Issue #4879: a plain index and the BY KEY / BY VALUE variants must coexist as distinct indexes.
   */
  @Test
  void plainAndByKeyByValueCoexist() {
    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Movie (thumbs BY KEY) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Movie (thumbs BY VALUE) NOTUNIQUE");
    });

    assertThat(database.getSchema().existsIndex("Movie[thumbsbykey]")).isTrue();
    assertThat(database.getSchema().existsIndex("Movie[thumbsbyvalue]")).isTrue();
  }

  /**
   * Issue #4880: after creating a BY KEY index, CONTAINSKEY queries must use it and return the
   * matching records.
   */
  @Test
  void queryByKeyUsesIndex() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Movie (thumbs BY KEY) NOTUNIQUE"));

    database.transaction(() -> {
      final Map<String, Object> t1 = new HashMap<>(Map.of(
          "poster", "p1.png",
          "banner", "b1.png"));
      final MutableDocument m1 = database.newDocument("Movie");
      m1.set("title", "M1");
      m1.set("thumbs", t1);
      m1.save();

      final Map<String, Object> t2 = new HashMap<>();
      t2.put("poster", "p2.png");
      final MutableDocument m2 = database.newDocument("Movie");
      m2.set("title", "M2");
      m2.set("thumbs", t2);
      m2.save();
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'banner'");
      final List<String> titles = rs.stream().map(r -> r.<String>getProperty("title")).toList();
      assertThat(titles).containsExactlyInAnyOrder("M1");

      final ResultSet rs2 = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'poster'");
      final List<String> titles2 = rs2.stream().map(r -> r.<String>getProperty("title")).toList();
      assertThat(titles2).containsExactlyInAnyOrder("M1", "M2");

      final String explain = database.query("sql", "EXPLAIN SELECT FROM Movie WHERE thumbs CONTAINSKEY 'banner'")
          .next().getProperty("executionPlan").toString();
      assertThat(explain).contains("Movie[thumbs");
    });
  }

  /**
   * Issue #4880: after creating a BY VALUE index, CONTAINSVALUE queries must use it and return the
   * matching records.
   */
  @Test
  void queryByValueUsesIndex() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Movie (thumbs BY VALUE) NOTUNIQUE"));

    database.transaction(() -> {
      final Map<String, Object> t1 = new HashMap<>();
      t1.put("poster", "shared.png");
      final MutableDocument m1 = database.newDocument("Movie");
      m1.set("title", "M1");
      m1.set("thumbs", t1);
      m1.save();

      final Map<String, Object> t2 = new HashMap<>(Map.of(
          "banner", "shared.png",
          "poster", "unique.png"));
      final MutableDocument m2 = database.newDocument("Movie");
      m2.set("title", "M2");
      m2.set("thumbs", t2);
      m2.save();
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSVALUE 'shared.png'");
      final List<String> titles = rs.stream().map(r -> r.<String>getProperty("title")).toList();
      assertThat(titles).containsExactlyInAnyOrder("M1", "M2");

      final ResultSet rs2 = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSVALUE 'unique.png'");
      final List<String> titles2 = rs2.stream().map(r -> r.<String>getProperty("title")).toList();
      assertThat(titles2).containsExactlyInAnyOrder("M2");
    });
  }

  /**
   * Issue #4880: the BY KEY index must be maintained on updates and deletes.
   */
  @Test
  void byKeyIndexMaintainedOnUpdateAndDelete() {
    database.transaction(() -> database.command("sql", "CREATE INDEX ON Movie (thumbs BY KEY) NOTUNIQUE"));

    database.transaction(() -> {
      final Map<String, Object> t1 = new HashMap<>();
      t1.put("poster", "p1.png");
      final MutableDocument m1 = database.newDocument("Movie");
      m1.set("title", "M1");
      m1.set("thumbs", t1);
      m1.save();
    });

    // Update: replace the key set (poster -> banner)
    database.transaction(() -> {
      final Result r = database.query("sql", "SELECT FROM Movie WHERE title = 'M1'").next();
      final MutableDocument m1 = r.getElement().get().getRecord().asDocument().modify();
      final Map<String, Object> t = new HashMap<>();
      t.put("banner", "b1.png");
      m1.set("thumbs", t);
      m1.save();
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'poster'");
      assertThat(rs.stream().count()).isEqualTo(0);
      final ResultSet rs2 = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'banner'");
      assertThat(rs2.stream().count()).isEqualTo(1);
    });

    // Delete
    database.transaction(() -> database.command("sql", "DELETE FROM Movie WHERE title = 'M1'"));
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'banner'");
      assertThat(rs.stream().count()).isEqualTo(0);
    });
  }

  /**
   * Issue #4880: creating a BY KEY / BY VALUE index on a type that already contains data must index
   * the existing records (initial build scan path).
   */
  @Test
  void byKeyByValueIndexBuiltOverExistingData() {
    database.transaction(() -> {
      final Map<String, Object> t1 = new HashMap<>(Map.of(
          "poster", "p1.png",
          "banner", "b1.png"));
      final MutableDocument m1 = database.newDocument("Movie");
      m1.set("title", "M1");
      m1.set("thumbs", t1);
      m1.save();

      final Map<String, Object> t2 = new HashMap<>();
      t2.put("poster", "shared.png");
      final MutableDocument m2 = database.newDocument("Movie");
      m2.set("title", "M2");
      m2.set("thumbs", t2);
      m2.save();
    });

    database.transaction(() -> {
      database.command("sql", "CREATE INDEX ON Movie (thumbs BY KEY) NOTUNIQUE");
      database.command("sql", "CREATE INDEX ON Movie (thumbs BY VALUE) NOTUNIQUE");
    });

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'banner'");
      assertThat(rs.stream().map(r -> r.<String>getProperty("title")).toList()).containsExactly("M1");

      final ResultSet rs2 = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSVALUE 'shared.png'");
      assertThat(rs2.stream().map(r -> r.<String>getProperty("title")).toList()).containsExactly("M2");

      final ResultSet rs3 = database.query("sql", "SELECT FROM Movie WHERE thumbs CONTAINSKEY 'poster'");
      assertThat(rs3.stream().map(r -> r.<String>getProperty("title")).toList()).containsExactlyInAnyOrder("M1", "M2");
    });
  }

  /**
   * Issue #4881: CREATE INDEX IF NOT EXISTS on an already existing BY ITEM/KEY/VALUE index must
   * return created=false instead of always true.
   */
  @Test
  void ifNotExistsReturnsCreatedFalseForByModifiers() {
    database.transaction(() -> {
      final DocumentType scans = database.getSchema().createDocumentType("scans");
      scans.createProperty("tags", Type.LIST);
    });

    database.transaction(() -> {
      final ResultSet first = database.command("sql", "CREATE INDEX IF NOT EXISTS ON scans (tags BY ITEM) NOTUNIQUE");
      assertThat(first.next().<Boolean>getProperty("created")).isTrue();

      final ResultSet second = database.command("sql", "CREATE INDEX IF NOT EXISTS ON scans (tags BY ITEM) NOTUNIQUE");
      assertThat(second.next().<Boolean>getProperty("created")).isFalse();
    });

    database.transaction(() -> {
      final ResultSet first = database.command("sql", "CREATE INDEX IF NOT EXISTS ON Movie (thumbs BY KEY) NOTUNIQUE");
      assertThat(first.next().<Boolean>getProperty("created")).isTrue();

      final ResultSet second = database.command("sql", "CREATE INDEX IF NOT EXISTS ON Movie (thumbs BY KEY) NOTUNIQUE");
      assertThat(second.next().<Boolean>getProperty("created")).isFalse();
    });
  }
}
