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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8871: EXPORT DATABASE writes an infinite FLOAT/DOUBLE as the "PosInfinity"/"NegInfinity" marker and IMPORT
 * DATABASE failed on it with a NumberFormatException; a DECIMAL also lost its precision on the way back in (read
 * through a double).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue8871JsonlNonFiniteAndDecimalRoundTripTest {
  private static final String SOURCE_PATH = "target/databases/issue8871-source";
  private static final String TARGET_PATH = "target/databases/issue8871-target";
  private static final String FILE        = "target/issue8871.jsonl.tgz";

  private static final BigDecimal BIG = new BigDecimal("12345678901234567890.123456789012345678");

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File(FILE).delete();
  }

  @Test
  void nonFiniteFloatsAndDoublesAndWideDecimalsSurviveExportImport() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE T");
      source.command("sql", "CREATE PROPERTY T.k INTEGER");
      source.command("sql", "CREATE PROPERTY T.f FLOAT");
      source.command("sql", "CREATE PROPERTY T.d DOUBLE");
      source.command("sql", "CREATE PROPERTY T.dec DECIMAL");
      source.command("sql", "CREATE PROPERTY T.decs LIST OF DECIMAL");
      source.command("sql", "CREATE PROPERTY T.ds LIST OF DOUBLE");
      source.command("sql", "CREATE PROPERTY T.fs LIST OF FLOAT");
      source.command("sql", "CREATE VERTEX TYPE V");
      source.command("sql", "CREATE PROPERTY V.d DOUBLE");
      source.command("sql", "CREATE PROPERTY V.dec DECIMAL");
      source.transaction(() -> {
        source.newDocument("T").set("k", 0, "f", Float.POSITIVE_INFINITY, "d", Double.POSITIVE_INFINITY, "dec", BIG).save();
        source.newDocument("T").set("k", 1, "f", Float.NEGATIVE_INFINITY, "d", Double.NEGATIVE_INFINITY, "decs", List.of(BIG)).save();
        source.newDocument("T").set("k", 2, "f", Float.NaN, "d", Double.NaN, "ds", List.of(1.5, Double.NEGATIVE_INFINITY, Double.NaN),
            "fs", List.of(Float.POSITIVE_INFINITY, 2.5f)).save();
        source.newVertex("V").set("d", Double.POSITIVE_INFINITY, "dec", BIG).save();
        // ordinary finite extremes must keep round-tripping unchanged
        source.newDocument("T").set("k", 3, "f", -0.0f, "d", Double.MAX_VALUE).save();
      });
      source.command("sql", "EXPORT DATABASE file://" + new File(FILE).getName() + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + new File(FILE).getName()).getAbsolutePath());

      assertThat(row(target, 0).<Float>getProperty("f")).isEqualTo(Float.POSITIVE_INFINITY);
      assertThat(row(target, 0).<Double>getProperty("d")).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(row(target, 0).<BigDecimal>getProperty("dec")).isEqualByComparingTo(BIG);
      assertThat(row(target, 1).<Float>getProperty("f")).isEqualTo(Float.NEGATIVE_INFINITY);
      assertThat(row(target, 1).<Double>getProperty("d")).isEqualTo(Double.NEGATIVE_INFINITY);
      final List<?> decs = row(target, 1).getProperty("decs");
      assertThat(decs.get(0)).isInstanceOf(BigDecimal.class);
      assertThat((BigDecimal) decs.get(0)).isEqualByComparingTo(BIG);
      final List<?> ds = row(target, 2).getProperty("ds");
      assertThat(ds.get(0)).isEqualTo(1.5);
      assertThat(ds.get(1)).isEqualTo(Double.NEGATIVE_INFINITY);
      assertThat((Double) ds.get(2)).isNaN();
      final List<?> fs = row(target, 2).getProperty("fs");
      assertThat(((Number) fs.get(0)).doubleValue()).isEqualTo(Double.POSITIVE_INFINITY);
      assertThat(((Number) fs.get(1)).floatValue()).isEqualTo(2.5f);
      try (final ResultSet rs = target.query("sql", "SELECT FROM V")) {
        final Result v = rs.next();
        assertThat(v.<Double>getProperty("d")).isEqualTo(Double.POSITIVE_INFINITY);
        assertThat(v.<BigDecimal>getProperty("dec")).isEqualByComparingTo(BIG);
      }
      assertThat(row(target, 2).<Float>getProperty("f")).isNaN();
      assertThat(row(target, 2).<Double>getProperty("d")).isNaN();
      assertThat(row(target, 3).<Float>getProperty("f")).isEqualTo(-0.0f);
      assertThat(row(target, 3).<Double>getProperty("d")).isEqualTo(Double.MAX_VALUE);
    }
  }

  private static Result row(final Database db, final int k) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM T WHERE k = ?", k)) {
      return rs.next();
    }
  }
}
