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
import com.arcadedb.database.Document;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12677: a native select range ({@code > >= < <= between}) on a property whose only index is a HASH index threw
 * {@code UnsupportedOperationException: ... does not support ordered iterations} where the same select without the index, with an
 * LSM_TREE index, or as SQL answers the rows. A hash index answers equality only (documented), so the range leaf is a scan.
 */
class SelectHashIndexRangeTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("P");
      type.createProperty("h", Type.INTEGER);
      type.createProperty("u", Type.INTEGER);
      type.createProperty("l", Type.INTEGER);
      type.createProperty("plain", Type.INTEGER);
      type.createTypeIndex(Schema.INDEX_TYPE.HASH, false, "h");
      type.createTypeIndex(Schema.INDEX_TYPE.HASH, true, "u");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "l");
      for (int i = 0; i < 10; i++)
        database.newDocument("P").set("h", i, "u", i, "l", i, "plain", i).save();
    });
  }

  private List<Integer> ids(final SelectIterator<Document> iterator, final String property) {
    final List<Integer> result = new ArrayList<>();
    while (iterator.hasNext())
      result.add(iterator.next().getInteger(property));
    result.sort(null);
    return result;
  }

  @Test
  void rangeOnANotUniqueHashIndexIsAScan() {
    assertThat(ids(database.select().fromType("P").where().property("h").gt().value(7).documents(), "h")).containsExactly(8, 9);
    assertThat(ids(database.select().fromType("P").where().property("h").ge().value(7).documents(), "h")).containsExactly(7, 8, 9);
    assertThat(ids(database.select().fromType("P").where().property("h").lt().value(2).documents(), "h")).containsExactly(0, 1);
    assertThat(ids(database.select().fromType("P").where().property("h").le().value(2).documents(), "h")).containsExactly(0, 1, 2);
    assertThat(ids(database.select().fromType("P").where().property("h").between().values(3, 5).documents(), "h")).containsExactly(3, 4,
        5);
  }

  @Test
  void rangeOnAUniqueHashIndexIsAScan() {
    assertThat(ids(database.select().fromType("P").where().property("u").gt().value(7).documents(), "u")).containsExactly(8, 9);
    assertThat(ids(database.select().fromType("P").where().property("u").le().value(1).documents(), "u")).containsExactly(0, 1);
    assertThat(database.select().fromType("P").where().property("u").between().values(3, 5).compile().count()).isEqualTo(3L);
    assertThat(database.select().fromType("P").where().property("u").gt().value(8).compile().exists()).isTrue();
    assertThat(database.select().fromType("P").where().property("u").gt().value(9).compile().exists()).isFalse();
  }

  @Test
  void rangeOnAHashIndexIsAnsweredByTheScanNotByTheIndex() {
    final SelectIterator<Document> gt = database.select().fromType("P").where().property("h").gt().value(7).documents();
    assertThat(ids(gt, "h")).containsExactly(8, 9);
    assertThat(gt.getMetrics().get("usedIndexes")).isEqualTo(0);

    final SelectIterator<Document> between = database.select().fromType("P").where().property("u").between().values(3, 5).documents();
    assertThat(ids(between, "u")).containsExactly(3, 4, 5);
    assertThat(between.getMetrics().get("usedIndexes")).isEqualTo(0);

    // an or with a hash range on one side cannot take the indexed side alone as the candidate set: both sides are scanned
    final SelectIterator<Document> or = database.select().fromType("P").where().property("h").gt().value(7).or().property("l").eq()
        .value(0).documents();
    assertThat(ids(or, "plain")).containsExactly(0, 8, 9);
    assertThat(or.getMetrics().get("usedIndexes")).isEqualTo(0);
  }

  @Test
  void rangeOnAHashIndexWithOrderByAndLimit() {
    final List<Integer> result = new ArrayList<>();
    final SelectIterator<Document> iterator = database.select().fromType("P").where().property("h").ge().value(4).orderBy("h", false)
        .limit(3).documents();
    while (iterator.hasNext())
      result.add(iterator.next().getInteger("h"));
    assertThat(result).containsExactly(9, 8, 7);
  }

  @Test
  void rangeOnTheHashIndexUnderAnOrKeepsTheOtherBranch() {
    // the hash leaf is not answerable by an index, so the or must not take the LSM branch alone as the candidate set
    assertThat(ids(database.select().fromType("P").where().property("h").gt().value(7).or().property("l").eq().value(0).documents(),
        "plain")).containsExactly(0, 8, 9);
    assertThat(ids(database.select().fromType("P").where().property("l").lt().value(2).or().property("h").ge().value(9).documents(),
        "plain")).containsExactly(0, 1, 9);
  }

  @Test
  void rangeOnTheHashIndexUnderAnAndStillFiltersWithTheOtherIndex() {
    assertThat(ids(database.select().fromType("P").where().property("h").gt().value(2).and().property("l").le().value(4).documents(),
        "plain")).containsExactly(3, 4);
  }

  @Test
  void equalityAndInOnTheHashIndexStillUseIt() {
    final SelectIterator<Document> eq = database.select().fromType("P").where().property("h").eq().value(4).documents();
    assertThat(ids(eq, "h")).containsExactly(4);
    assertThat(eq.getMetrics().get("usedIndexes")).isEqualTo(1);

    final SelectIterator<Document> in = database.select().fromType("P").where().property("h").in().value(List.of(1, 3)).documents();
    assertThat(ids(in, "h")).containsExactly(1, 3);
    assertThat(in.getMetrics().get("usedIndexes")).isEqualTo(1);
  }

  @Test
  void rangeOnALsmIndexStillUsesIt() {
    final SelectIterator<Document> range = database.select().fromType("P").where().property("l").gt().value(7).documents();
    assertThat(ids(range, "l")).containsExactly(8, 9);
    assertThat(range.getMetrics().get("usedIndexes")).isEqualTo(1);
  }
}
