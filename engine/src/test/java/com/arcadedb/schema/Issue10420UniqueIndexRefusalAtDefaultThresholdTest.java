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
package com.arcadedb.schema;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.index.IndexException;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10420: from 10,000 records on, {@code CREATE INDEX ... UNIQUE} switches to the sorted build, and a duplicate key was
 * refused with "Cannot build sorted index" instead of the message of the record-at-a-time build below that count, so an embedded
 * caller no longer saw which key and which record clash. #10382 fixed the sorted build (and tests it against a lowered
 * threshold); this pins the exact shape of the report on the DEFAULT threshold, one record either side of it: the same statement
 * is refused with the same message, for a plain key, a composite key and a duplicate held by a sub type.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10420UniqueIndexRefusalAtDefaultThresholdTest extends TestHelper {
  /** One record either side of {@link GlobalConfiguration#INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS}, which is 10,000 by default. */
  private static final long ONE_BELOW_THE_THRESHOLD = 9_999L;
  private static final long AT_THE_THRESHOLD        = 10_000L;

  @ParameterizedTest(name = "{0} records")
  @ValueSource(longs = { ONE_BELOW_THE_THRESHOLD, AT_THE_THRESHOLD })
  void duplicateKeyIsNamedInTheMessage(final long records) {
    assertThat(GlobalConfiguration.INDEX_BUILD_SORTED_UNIQUE_MIN_RECORDS.getValueAsLong()).isEqualTo(AT_THE_THRESHOLD);
    createType("P", null);
    insertDistinct("P", records - 1);
    database.transaction(() -> database.newDocument("P").set("k", key(0)).save());

    assertRefusedNaming("P", "CREATE INDEX ON P (k) UNIQUE", "properties [k]", "Duplicated key [" + key(0) + "] found on index 'P[k]'");
    assertThat(database.getSchema().getType("P").getIndexByProperties("k")).isNull();
  }

  @ParameterizedTest(name = "{0} records")
  @ValueSource(longs = { ONE_BELOW_THE_THRESHOLD, AT_THE_THRESHOLD })
  void duplicateHeldByASubTypeIsNamedInTheMessage(final long records) {
    createType("P", null);
    createType("Q", "P");
    insertDistinct("P", records - 1);
    database.transaction(() -> database.newDocument("Q").set("k", key(5)).save());

    assertRefusedNaming("P", "CREATE INDEX ON P (k) UNIQUE", "properties [k]", "Duplicated key [" + key(5) + "] found on index 'P[k]'");
  }

  @ParameterizedTest(name = "{0} records")
  @ValueSource(longs = { ONE_BELOW_THE_THRESHOLD, AT_THE_THRESHOLD })
  void duplicateCompositeKeyIsNamedInTheMessage(final long records) {
    final DocumentType type = database.getSchema().buildDocumentType().withName("C").withTotalBuckets(3).create();
    type.createProperty("a", Type.INTEGER);
    type.createProperty("b", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < records - 1; i++)
        database.newDocument("C").set("a", i % 10).set("b", i).save();
      database.newDocument("C").set("a", 7).set("b", 7).save();
    });

    assertRefusedNaming("C", "CREATE INDEX ON C (a, b) UNIQUE", "properties [a, b]", "Duplicated key [7, 7] found on index 'C[a,b]'");
  }

  private void assertRefusedNaming(final String typeName, final String statement, final String properties, final String duplicate) {
    assertThatThrownBy(() -> database.command("sql", statement).close())
        .isInstanceOf(IndexException.class)
        .hasMessageStartingWith("Error on creating index on type '" + typeName + "', " + properties)
        .hasMessageContaining(duplicate + " already assigned to record #")
        .hasMessageNotContaining("Cannot build sorted index")
        .hasRootCauseInstanceOf(DuplicatedKeyException.class);
  }

  private void createType(final String typeName, final String superTypeName) {
    final DocumentType type = database.getSchema().buildDocumentType().withName(typeName).withTotalBuckets(3).create();
    if (superTypeName != null)
      type.addSuperType(superTypeName);
    else
      type.createProperty("k", Type.STRING);
  }

  private void insertDistinct(final String typeName, final long records) {
    for (long from = 0; from < records; from += 10_000L) {
      final long first = from;
      database.transaction(() -> {
        for (long i = first; i < Math.min(records, first + 10_000L); i++)
          database.newDocument(typeName).set("k", key(i)).save();
      });
    }
  }

  private static String key(final long i) {
    return "k" + i;
  }
}
