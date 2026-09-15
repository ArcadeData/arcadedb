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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #7581: {@code ALTER TYPE <timeseries> SUPERTYPE ...} let a polymorphic property reach
 * a TIMESERIES type through inheritance rather than {@code CREATE PROPERTY}, which is the same silent-drop shape
 * issue #7567 refused for a property declared directly on the type. {@code LocalTimeSeriesType.tsColumns} is
 * filled once by {@code TimeSeriesTypeBuilder.create()} and has no other caller, so a super type linked afterwards
 * cannot extend it: the time-series write path ({@code SaveElementStep#saveToTimeSeries}) reads the document under
 * the declared column names only and silently drops a value written under an inherited property name.
 * <p>
 * Refused from both ends: a TIMESERIES type may not gain a super type, and may not become anyone else's super type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7581TimeSeriesSupertypeTest extends TestHelper {

  private void createTimeSeriesType() {
    database.command("sql", "CREATE TIMESERIES TYPE R TIMESTAMP ts TAGS (s STRING) FIELDS (v DOUBLE)");
  }

  @Test
  void timeSeriesTypeCannotReceiveASuperType() {
    createTimeSeriesType();
    database.command("sql", "CREATE DOCUMENT TYPE Base");
    database.command("sql", "CREATE PROPERTY Base.humidity DOUBLE");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE R SUPERTYPE +Base"))
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType("R").getSuperTypes()).isEmpty();
  }

  @Test
  void timeSeriesTypeCannotBecomeASuperType() {
    createTimeSeriesType();
    database.command("sql", "CREATE DOCUMENT TYPE Sub");

    assertThatThrownBy(() -> database.command("sql", "ALTER TYPE Sub SUPERTYPE +R"))
        .hasMessageContaining("TIMESERIES");

    assertThat(database.getSchema().getType("Sub").getSuperTypes()).isEmpty();
  }

  @Test
  void javaApiAlsoRefusesBothDirections() {
    createTimeSeriesType();
    final DocumentType r = database.getSchema().getType("R");
    final DocumentType base = database.getSchema().buildDocumentType().withName("Base2").create();

    assertThatThrownBy(() -> r.addSuperType(base)).hasMessageContaining("TIMESERIES");
    assertThatThrownBy(() -> base.addSuperType(r)).hasMessageContaining("TIMESERIES");
  }

  /**
   * Control: two ordinary document types must still be free to form a hierarchy - the refusal above must not be
   * an accidental blanket refusal of SUPERTYPE.
   */
  @Test
  void twoOrdinaryTypesCanStillFormAHierarchy() {
    database.command("sql", "CREATE DOCUMENT TYPE Animal");
    database.command("sql", "CREATE DOCUMENT TYPE Dog");
    database.command("sql", "ALTER TYPE Dog SUPERTYPE +Animal");

    assertThat(database.getSchema().getType("Dog").getSuperTypes())
        .extracting(DocumentType::getName)
        .containsExactly("Animal");
  }
}
