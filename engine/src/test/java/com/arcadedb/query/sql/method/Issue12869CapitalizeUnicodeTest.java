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
package com.arcadedb.query.sql.method;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12869: {@code capitalize()} cut words at ASCII boundaries only ({@code café} became {@code CafÉ}, Cyrillic was untouched)
 * and read a {@code $} or a backslash of the text as replacement syntax, which threw.
 */
class Issue12869CapitalizeUnicodeTest extends TestHelper {
  @Test
  void aNonAsciiWordIsCapitalizedAsAWhole() {
    assertThat(value("SELECT \"café au lait\".capitalize() AS v")).isEqualTo("Café Au Lait");
    assertThat(value("SELECT \"über cool\".capitalize() AS v")).isEqualTo("Über Cool");
    assertThat(value("SELECT \"élan vital\".capitalize() AS v")).isEqualTo("Élan Vital");
    assertThat(value("SELECT \"ÉLAN VITAL\".capitalize() AS v")).isEqualTo("Élan Vital");
    assertThat(value("SELECT \"naïve résumé\".capitalize() AS v")).isEqualTo("Naïve Résumé");
    assertThat(value("SELECT \"привет мир\".capitalize() AS v")).isEqualTo("Привет Мир");
    assertThat(value("SELECT \"straße\".capitalize() AS v")).isEqualTo("Straße");
  }

  @Test
  void aDollarOrABackslashInTheTextIsData() {
    assertThat(value("SELECT \"a$b\".capitalize() AS v")).isEqualTo("A$B");
    assertThat(value("SELECT \"price: $5 each\".capitalize() AS v")).isEqualTo("Price: $5 Each");
    assertThat(value("SELECT \"x ${y} z\".capitalize() AS v")).isEqualTo("X ${Y} Z");
    assertThat(value("SELECT \"c:\\\\temp\".capitalize() AS v")).isEqualTo("C:\\Temp");
  }

  @Test
  void asciiTextIsUnchanged() {
    assertThat(value("SELECT \"hello world\".capitalize() AS v")).isEqualTo("Hello World");
    assertThat(value("SELECT \"HELLO WORLD\".capitalize() AS v")).isEqualTo("Hello World");
    assertThat(value("SELECT \"111 222\".capitalize() AS v")).isEqualTo("111 222");
    assertThat(value("SELECT \"\".capitalize() AS v")).isEqualTo("");
  }

  private Object value(final String sql) {
    try (final ResultSet rs = database.query("sql", sql)) {
      return rs.next().getProperty("v");
    }
  }
}
