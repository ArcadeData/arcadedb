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
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.index.EmptyIndexCursor;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.MultiIndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * https://github.com/ArcadeData/arcadedb/issues/8153
 * <p>
 * {@link MultiIndexCursor} sampled the key types and the comparator it merges with from the FIRST child cursor. An
 * equality lookup ({@code TypeIndex.get()}) answered that question with an empty key-type array and a null comparator,
 * so a WHERE starting with an {@code =} leaf followed by two range leaves on the same indexed property crashed the
 * k-way merge with an {@link ArrayIndexOutOfBoundsException} instead of answering.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class Issue8153SelectKeylessFirstCursorTest extends TestHelper {

  @Override
  protected void beginTest() {
    final DocumentType type = database.getSchema().createDocumentType("T");
    type.createProperty("b", Type.INTEGER);
    database.transaction(() -> {
      for (int i = 0; i < 5; i++) {
        final MutableDocument doc = database.newDocument("T");
        doc.set("b", i);
        doc.save();
      }
    });
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "b");
  }

  @Test
  void equalityLeafFirstFollowedByTwoRangeLeaves() {
    assertThat(database.select().json(new JSONObject(
            "{\"fromType\":\"T\",\"where\":[[[\":b\",\"=\",1],\"and\",[\":b\",\">=\",1]],\"and\",[\":b\",\"<=\",1]]}"))
        .count()).isEqualTo(1);

    assertThat(database.select().fromType("T").where().property("b").eq().value(1).and().property("b").ge().value(1).and()
        .property("b").le().value(1).count()).isEqualTo(1);
  }

  @Test
  void sameLeavesInAnotherOrderAgree() {
    assertThat(database.select().fromType("T").where().property("b").ge().value(1).and().property("b").le().value(1).and()
        .property("b").eq().value(1).count()).isEqualTo(1);
    assertThat(database.select().fromType("T").where().property("b").eq().value(1).or().property("b").ge().value(3).or()
        .property("b").le().value(0).count()).isEqualTo(4);
  }

  @Test
  void orAcrossIndexesOfDifferentKeyTypesIsAUnionNotAKeyMerge() {
    // THE LEAVES OF AN or CAN BE ANSWERED BY DIFFERENT INDEXES, WHOSE KEYS ARE NOT COMPARABLE: MERGING THEM BY KEY
    // COMPARED AN INTEGER UNDER A STRING KEY TYPE AS SOON AS THREE CURSORS WITH REAL KEYS WERE LIVE
    final DocumentType type = database.getSchema().getType("T");
    type.createProperty("s", Type.STRING);
    database.transaction(() -> database.command("sql", "UPDATE T SET s = 'v' + b"));
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "s");

    assertThat(database.select().fromType("T").where().property("b").ge().value(3).or().property("s").le().value("v0").or()
        .property("b").eq().value(1).count()).isEqualTo(4);
    assertThat(database.select().fromType("T").where().property("s").eq().value("v2").or().property("b").ge().value(4).or()
        .property("s").ge().value("v3").count()).isEqualTo(3);
  }

  @Test
  void inLeafServesAnAscendingOrderByInKeyOrder() {
    // A NESTED in_op CURSOR MERGES EQUALITY LOOKUPS OVER ONE INDEX, AND WITH A SINGLE USED INDEX SelectIterator SKIPS
    // ITS IN-MEMORY SORT: THE LOOKUPS USED TO CARRY NO KEY, SO THEY CAME BACK IN THE ORDER OF THE IN LIST
    final List<Integer> values = new ArrayList<>();
    database.select().fromType("T").where().property("b").in().value(List.of(4, 0, 2)).orderBy("b", true).documents()
        .forEachRemaining(d -> values.add(d.getInteger("b")));
    assertThat(values).containsExactly(0, 2, 4);
  }

  @Test
  void equalityLookupCarriesTheIndexKeyMetadata() {
    final TypeIndex index = (TypeIndex) database.getSchema().getType("T").getIndexesByProperties("b").getFirst();
    final IndexCursor cursor = index.get(new Object[] { 1 });
    assertThat(cursor.getBinaryKeyTypes()).isEqualTo(index.getBinaryKeyTypes());
    assertThat(cursor.getComparator()).isNotNull();
    assertThat(cursor.getKeys()).containsExactly(1);
  }

  @Test
  void sqlIndexLookupProjectsTheKeyItMatched() {
    // FetchFromIndexStep PROJECTS cursor.getKeys(): AN EQUALITY LOOKUP ON A TYPE INDEX USED TO ANSWER key = []
    final String indexName = database.getSchema().getType("T").getIndexesByProperties("b").getFirst().getName();
    try (final ResultSet rs = database.query("sql", "SELECT FROM index:`" + indexName + "` WHERE key = 1")) {
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("key")).isEqualTo(new Object[] { 1 });
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void mergeSamplesKeyMetadataFromTheFirstChildThatHasIt() {
    final TypeIndex index = (TypeIndex) database.getSchema().getType("T").getIndexesByProperties("b").getFirst();

    final List<IndexCursor> children = new ArrayList<>();
    children.add(new EmptyIndexCursor());
    children.add(index.range(true, new Object[] { 3 }, true, null, false));
    children.add(index.range(true, null, false, new Object[] { 1 }, true));

    final List<Object> keys = new ArrayList<>();
    try (final MultiIndexCursor merged = new MultiIndexCursor(children, -1, true)) {
      assertThat(merged.getBinaryKeyTypes()).isEqualTo(index.getBinaryKeyTypes());
      assertThat(merged.getComparator()).isNotNull();
      while (merged.hasNext()) {
        final Identifiable next = merged.next();
        assertThat(next).isNotNull();
        keys.add(merged.getKeys()[0]);
      }
    }
    assertThat(keys).containsExactly(0, 1, 3, 4);
  }
}
