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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.CountingImmutableDocument;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.schema.DocumentType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13012: {@link ResultInternal#isPlainRecordRow(Result)} is true exactly for the rows whose
 * {@link ResultInternal#propertiesOf(Result)} keys are the names of the record behind the row, the projection's exclusions left out,
 * in the order the record lists them, so that a consumer may take those keys instead of listing the record's names a second time.
 * It must be false for every other shape, whose names are not the record's.
 */
class ResultInternalIsPlainRecordRowTest extends TestHelper {
  private static final List<String> NAMES = List.of("a", "b", "c", "nothing");

  private RID rid;

  @BeforeEach
  void createRecord() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Row");
      final MutableDocument doc = database.newDocument("Row");
      doc.set("a", 1).set("b", "two").set("c", List.of(3, 4)).set("nothing", null);
      rid = doc.save().getIdentity();
    });
  }

  @Test
  void aRowOverARecordThatHoldsNothingOfItsOwnIsPlain() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(rid.asDocument(true));

      assertThat(ResultInternal.isPlainRecordRow(row)).isTrue();
      assertThat(ResultInternal.propertiesOf(row).keySet()).containsExactlyElementsOf(NAMES);
    });
  }

  @Test
  void aRowWhoseContentIsEmptyIsPlain() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(Collections.<String, Object>emptyMap()).setElement(rid.asDocument(true));

      assertThat(ResultInternal.isPlainRecordRow(row)).isTrue();
      assertThat(ResultInternal.propertiesOf(row).keySet()).containsExactlyElementsOf(NAMES);
    });
  }

  @Test
  void theNamesAProjectionExcludedAreLeftOutOfTheKeysOfAPlainRow() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(rid.asDocument(true)).setProjectionExcludes(Set.of("b"));

      assertThat(ResultInternal.isPlainRecordRow(row)).isTrue();
      assertThat(ResultInternal.propertiesOf(row).keySet()).containsExactly("a", "c", "nothing");
    });
  }

  @Test
  void aRowWithoutARecordIsNotPlain() {
    assertThat(ResultInternal.isPlainRecordRow(new ResultInternal(database))).isFalse();
    assertThat(ResultInternal.isPlainRecordRow(new ResultInternal(Map.of("k", 1)))).isFalse();
  }

  @Test
  void aRowThatHoldsValuesOfItsOwnIsNotPlain() {
    database.transaction(() -> {
      // a projection row as the executor builds one: its own values, with the record it was projected from kept as the element
      final ResultInternal row = new ResultInternal(database);
      row.setElement(rid.asDocument(true));
      row.setProperty("x", 10);

      assertThat(ResultInternal.isPlainRecordRow(row)).isFalse();
      assertThat(ResultInternal.propertiesOf(row).keySet()).as("the row's own names, not the record's").containsExactly("x");
    });
  }

  @Test
  void aRowFromWhichAPropertyWasRemovedIsNotPlain() {
    database.transaction(() -> {
      final ResultInternal row = new ResultInternal(rid.asDocument(true));
      row.removeProperty("b");

      assertThat(ResultInternal.isPlainRecordRow(row)).isFalse();
      assertThat(ResultInternal.propertiesOf(row).keySet()).as("the record still lists the removed name").doesNotContain("b");
    });
  }

  @Test
  void aSubclassIsNotPlain() {
    database.transaction(() -> {
      final ResultInternal subclass = new ResultInternal(rid.asDocument(true)) {
        @Override
        public Set<String> getPropertyNames() {
          return Set.of("answered by the subclass");
        }
      };

      assertThat(ResultInternal.isPlainRecordRow(subclass)).isFalse();
      assertThat(ResultInternal.propertiesOf(subclass).keySet()).containsExactly("answered by the subclass");
    });
  }

  @Test
  void aResultThatIsNotAResultInternalIsNotPlain() {
    assertThat(ResultInternal.isPlainRecordRow(new EmptyResult())).isFalse();
  }

  @Test
  void aDamagedRecordKeepsTheNameOfAValueItCannotDecode() {
    database.transaction(() -> {
      // the single read skips (and logs) a value it cannot decode, as BinarySerializer.deserializeProperties does for a corrupted
      // property: the row still lists the name, with a null value, and a consumer taking the keys announces the same column
      final CountingImmutableDocument document = new CountingImmutableDocument(database, type(), rid) {
        @Override
        public Map<String, Object> toMap(final boolean includeMetadata) {
          final Map<String, Object> read = super.toMap(includeMetadata);
          read.remove("b");
          return read;
        }
      };
      final ResultInternal row = new ResultInternal(document);

      final Map<String, Object> properties = ResultInternal.propertiesOf(row);

      assertThat(ResultInternal.isPlainRecordRow(row)).isTrue();
      assertThat(new ArrayList<>(properties.keySet())).containsExactlyElementsOf(NAMES);
      assertThat(properties.get("b")).isNull();
      assertThat(properties.get("a")).isEqualTo(1);
    });
  }

  private DocumentType type() {
    return database.getSchema().getType("Row");
  }
}
