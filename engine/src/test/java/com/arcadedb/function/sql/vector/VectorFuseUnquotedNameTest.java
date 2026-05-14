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
package com.arcadedb.function.sql.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #4233: calling {@code vector.fuse(...)} without
 * back-ticks throws "Unknown method name: fuse". Namespaced vector functions
 * must be callable using the canonical {@code vector.<name>} syntax (and the
 * other documented namespaces too), without forcing users to quote the name.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class VectorFuseUnquotedNameTest extends TestHelper {

  private static final String TYPE_NAME  = "FuseDoc4233";
  private static final String DENSE_IDX  = "FuseDoc4233[dense]";
  private static final String SPARSE_IDX = "FuseDoc4233[tokens,weights]";

  @Test
  void propertyDotMethodKeepsWorkingForRegularMethods() {
    // The fix must not break the regular identifier + .method(...) path. `source.asString()` is a
    // legitimate string method on a property; no SQL function called `source.asString` exists, so
    // the executor must still treat this as a method call.
    buildSchema();
    seedFixture();

    try (ResultSet rs = database.query("sql",
        "SELECT source.asString() AS s FROM " + TYPE_NAME + " WHERE source = 'A'")) {
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("s")).isEqualTo("A");
    }
  }

  @Test
  void unknownNamespacedFunctionStillReportsMethodFailure() {
    // A name that is neither a registered function nor a SQL method should still surface a clear
    // error so users see typos.
    buildSchema();
    seedFixture();

    assertThatThrownBy(() -> {
      try (ResultSet rs = database.query("sql",
          "SELECT vector.thisDoesNotExist() FROM " + TYPE_NAME)) {
        while (rs.hasNext()) rs.next();
      }
    }).hasMessageContaining("thisDoesNotExist");
  }

  @Test
  void vectorFuseInChainedAccessReturnsField() {
    // After the function call, a follow-up `.size()` method must keep working: the namespaced
    // call must dispatch to the function, and any trailing modifier chain (modifier.next) must run
    // against the function result.
    buildSchema();
    seedFixture();

    try (ResultSet rs = database.query("sql", """
        SELECT vector.fuse(
            vector.neighbors(?, ?, ?),
            vector.sparseNeighbors(?, ?, ?, ?),
            { fusion: 'RRF' }
        ).size() AS s""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5)) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(((Number) rs.next().getProperty("s")).intValue()).isGreaterThan(0);
    }
  }

  @Test
  void vectorFuseCanBeCalledWithoutBackticks() {
    buildSchema();
    seedFixture();

    // The user-reported syntax: no back-ticks anywhere. This must NOT throw
    // "Unknown method name: fuse".
    try (ResultSet rs = database.query("sql", """
        SELECT expand(vector.fuse(
            vector.neighbors(?, ?, ?),
            vector.sparseNeighbors(?, ?, ?, ?),
            { fusion: 'RRF' }
        ))""",
        DENSE_IDX, new float[] { 1.0f, 0.0f, 0.0f }, 5,
        SPARSE_IDX, new int[] { 1 }, new float[] { 1.0f }, 5)) {
      int count = 0;
      while (rs.hasNext()) {
        rs.next();
        count++;
      }
      assertThat(count).as("fused rows").isGreaterThan(0);
    }
  }

  private void buildSchema() {
    database.transaction(() -> {
      final DocumentType t = database.getSchema().createDocumentType(TYPE_NAME);
      t.createProperty("dense", Type.ARRAY_OF_FLOATS);
      t.createProperty("tokens", Type.ARRAY_OF_INTEGERS);
      t.createProperty("weights", Type.ARRAY_OF_FLOATS);
      t.createProperty("source", Type.STRING);

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "dense" })
          .withLSMVectorType()
          .withDimensions(3)
          .withSimilarity("COSINE")
          .create();

      database.getSchema()
          .buildTypeIndex(TYPE_NAME, new String[] { "tokens", "weights" })
          .withSparseVectorType()
          .withDimensions(50)
          .create();
    });
  }

  private void seedFixture() {
    database.transaction(() -> {
      newDoc("A", new float[] { 1.0f, 0.0f, 0.0f }, new int[] { 1, 5 }, new float[] { 0.9f, 0.5f });
      newDoc("B", new float[] { 0.95f, 0.1f, 0.05f }, new int[] { 1, 6 }, new float[] { 0.4f, 0.5f });
      newDoc("C", new float[] { 0.5f, 0.5f, 0.0f }, new int[] { 1, 7 }, new float[] { 0.3f, 0.5f });
    });
  }

  private void newDoc(final String src, final float[] dense, final int[] tokens, final float[] weights) {
    final MutableDocument d = database.newDocument(TYPE_NAME);
    d.set("source", src);
    d.set("dense", dense);
    d.set("tokens", tokens);
    d.set("weights", weights);
    d.save();
  }
}
