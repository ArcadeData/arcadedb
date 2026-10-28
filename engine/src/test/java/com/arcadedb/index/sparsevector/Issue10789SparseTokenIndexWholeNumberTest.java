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
package com.arcadedb.index.sparsevector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.RID;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10789: the {@code LSM_SPARSE_VECTOR} write path converted the dimension ids of a record with {@code intValue()}, so
 * a list {@code [0, 2.5]} was indexed as dimension 2 and {@code 5000000000} as the wrapped int 705032704, while the record
 * itself kept what it was given. A declared {@code ARRAY_OF_INTEGERS} property converts on write and never reaches that code
 * with a list; a row stored before its property was declared does, when the index is built over it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10789SparseTokenIndexWholeNumberTest extends TestHelper {

  @Test
  void buildRefusesARowWhoseDimensionIsNotAWholeNumber() {
    final RID rid = storeRawRow("Frac", "frac", List.of(0, 2.5));

    assertThatThrownBy(() -> createIndex("Frac"))
        .isInstanceOf(IndexException.class)
        .hasMessageContaining(rid.toString())
        .hasMessageContaining("'tokens'")
        .hasMessageContaining("'2.5'");
  }

  @Test
  void buildRefusesARowWhoseDimensionDoesNotFitAnInt() {
    // 5000000000 wraps to the POSITIVE int 705032704: no ">= 0" check could catch it, only the conversion can
    storeRawRow("Wide", "wide", List.of(0, 5_000_000_000L));

    assertThatThrownBy(() -> createIndex("Wide"))
        .isInstanceOf(IndexException.class)
        .hasMessageContaining("'5000000000'");
  }

  @Test
  void buildIndexesAWholeNumberGivenAsADecimal() {
    // 2.0 is the dimension 2, whatever its Java type: refusing it would refuse every JSON client that writes "2.0"
    storeRawRow("Whole", "whole", List.of(0, 2.0d));
    createIndex("Whole");

    assertThat(neighbors("Whole", 2)).containsExactly("whole");
  }

  @Test
  void putRefusesAFractionalDimension() {
    createType("Put");
    createIndex("Put");
    final LSMSparseVectorIndex index = sparseIndex("Put");
    final RID rid = new RID(index.getAssociatedBucketId(), 0L);

    database.transaction(() -> assertThatThrownBy(
        () -> index.put(new Object[] { List.of(0, 2.5), List.of(0.5f, 0.9f) }, new RID[] { rid }))
        .isInstanceOf(IndexException.class)
        .hasMessageContaining(rid.toString()));
  }

  @Test
  void removeRetractsThePostingsATruncatedListWasIndexedWith() {
    // What a record stored before the fix carries: a fractional list, indexed as the truncated dimension. Its delete must
    // not be refused, and it has to retract the very posting the old put() wrote, or the dimension answers a ghost
    createType("Old");
    createIndex("Old");
    final LSMSparseVectorIndex index = sparseIndex("Old");
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("Old").set("name", "old").save().getIdentity());
    database.transaction(() -> index.put(new Object[] { new int[] { 0, 2 }, new float[] { 0.5f, 0.9f } }, rid));
    assertThat(neighbors("Old", 2)).containsExactly("old");

    database.transaction(() -> index.remove(new Object[] { List.of(0, 2.5), List.of(0.5f, 0.9f) }, rid[0]));

    assertThat(neighbors("Old", 2)).isEmpty();
  }

  @Test
  void aDeclaredPropertyStillConvertsOnWrite() {
    // The control: a declared ARRAY_OF_INTEGERS converts the list before any index sees it, as an INTEGER property converts
    // 2.5 to 2. That is the schema's conversion rule for every integer property and is left as it is
    createType("Decl");
    createIndex("Decl");
    database.transaction(() -> database.command("sql", "INSERT INTO Decl CONTENT {name:'decl',tokens:[0,2.5],weights:[0.5,0.9]}"));

    assertThat(neighbors("Decl", 2)).containsExactly("decl");
  }

  private RID storeRawRow(final String type, final String name, final List<?> tokens) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    final RID[] rid = new RID[1];
    database.transaction(
        () -> rid[0] = database.newDocument(type).set("name", name, "tokens", tokens, "weights", List.of(0.5f, 0.9f)).save()
            .getIdentity());
    database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
    return rid[0];
  }

  private void createType(final String type) {
    database.command("sql", "CREATE DOCUMENT TYPE " + type);
    database.command("sql", "CREATE PROPERTY " + type + ".tokens ARRAY_OF_INTEGERS");
    database.command("sql", "CREATE PROPERTY " + type + ".weights ARRAY_OF_FLOATS");
  }

  private void createIndex(final String type) {
    database.getSchema().buildTypeIndex(type, new String[] { "tokens", "weights" }).withType(Schema.INDEX_TYPE.LSM_SPARSE_VECTOR)
        .create();
  }

  private LSMSparseVectorIndex sparseIndex(final String type) {
    final TypeIndex typeIndex = (TypeIndex) database.getSchema().getIndexByName(type + "[tokens,weights]");
    return (LSMSparseVectorIndex) typeIndex.getIndexesOnBuckets()[0];
  }

  private List<String> neighbors(final String type, final int dimension) {
    final List<String> names = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT expand(`vector.sparseNeighbors`(?, ?, ?, ?))",
        type + "[tokens,weights]", new int[] { dimension }, new float[] { 1.0f }, 10)) {
      while (rs.hasNext())
        names.add(rs.next().getProperty("name"));
    }
    return names;
  }
}
