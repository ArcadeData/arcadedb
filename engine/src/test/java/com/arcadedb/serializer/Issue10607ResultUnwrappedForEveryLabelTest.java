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
package com.arcadedb.serializer;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #10607: {@link BinaryTypes#getTypeFromValue} routes a {@link Result} to one of three labels - {@code TYPE_RID} for a record,
 * {@code TYPE_MAP} for a row that is not a record, {@code TYPE_EMBEDDED} for an element that carries no RID - and the
 * serializer unwrapped it for two of them only (#10264 fixed the map). The third cast the {@code Result} to a
 * {@code Document} and failed with a {@code ClassCastException}, so a projected subquery that yields a newly built element
 * could not be stored by {@code INSERT ... FROM}, {@code CREATE MATERIALIZED VIEW} or an {@code INSERT ... SET}.
 * <p>
 * The test is parameterised over the three kinds of {@code Result}, over every place a value is serialised from (a plain
 * property, a list entry, a map value) and over the statements that hand a {@code Result} to the serializer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10607ResultUnwrappedForEveryLabelTest extends TestHelper {

  private enum Kind {
    /** a record with an identity: typed {@code TYPE_RID} */
    RECORD,
    /** a row that is not a record: typed {@code TYPE_MAP} */
    ROW,
    /** an element that was never saved: typed {@code TYPE_EMBEDDED} */
    NEW_ELEMENT
  }

  private RID savedRid;

  @BeforeEach
  void schema() {
    database.getSchema().createDocumentType("Emb");
    database.getSchema().createDocumentType("Holder").createProperty("emb", Type.EMBEDDED).setOfType("Emb");
    database.getSchema().createDocumentType("Sink");
    database.getSchema().createDocumentType("Base").createProperty("id", Type.INTEGER);
    database.getSchema().createDocumentType("Owner");
    database.transaction(() -> {
      savedRid = database.newDocument("Owner").set("k", 7).save().getIdentity();
      for (int i = 0; i < 3; i++) {
        final MutableDocument holder = database.newDocument("Holder");
        holder.newEmbeddedDocument("Emb", "emb").set("x", i);
        holder.save();
        database.newDocument("Base").set("id", i).save();
      }
    });
  }

  private Result resultOf(final Kind kind) {
    return switch (kind) {
      case RECORD -> new ResultInternal(savedRid.asDocument());
      case ROW -> new ResultInternal(new HashMap<>(Map.of("x", 1)));
      case NEW_ELEMENT -> {
        final MutableEmbeddedDocument element = database.newDocument("Sink").newEmbeddedDocument("Emb", "tmp");
        element.set("x", 1);
        yield new ResultInternal(element);
      }
    };
  }

  /** the value the stored {@code Result} must read back as: the record, or the properties of the row or element */
  private void assertReadsBack(final Kind kind, final Object stored) {
    switch (kind) {
    case RECORD -> {
      assertThat(stored).isInstanceOfAny(RID.class, Document.class);
      assertThat(((Identifiable) stored).getIdentity()).isEqualTo(savedRid);
    }
    case ROW -> assertThat(stored).isInstanceOf(Map.class).extracting(m -> ((Map<?, ?>) m).get("x")).isEqualTo(1);
    case NEW_ELEMENT -> {
      assertThat(stored).isInstanceOf(Document.class);
      assertThat(((Document) stored).getInteger("x")).isEqualTo(1);
    }
    }
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void aResultAsAPlainPropertyIsStored(final Kind kind) {
    final RID[] sink = new RID[1];
    database.transaction(() -> sink[0] = database.newDocument("Sink").set("value", resultOf(kind)).save().getIdentity());

    assertReadsBack(kind, sink[0].asDocument().get("value"));
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void aResultInAListIsStored(final Kind kind) {
    final RID[] sink = new RID[1];
    database.transaction(() -> sink[0] = database.newDocument("Sink").set("values", List.of(resultOf(kind))).save().getIdentity());

    final List<?> values = sink[0].asDocument().getList("values");
    assertThat(values).hasSize(1);
    assertReadsBack(kind, values.getFirst());
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void aResultAsAMapValueIsStored(final Kind kind) {
    final RID[] sink = new RID[1];
    database.transaction(() -> sink[0] = database.newDocument("Sink").set("values", Map.of("k", resultOf(kind))).save().getIdentity());

    final Map<String, Object> values = sink[0].asDocument().getMap("values");
    assertThat(values).hasSize(1);
    assertReadsBack(kind, values.get("k"));
  }

  /** one subquery per kind of {@code Result}: a record, a row, and a newly built element (an embedded document, expanded) */
  private static String subqueryOf(final Kind kind) {
    return switch (kind) {
      case RECORD -> "(SELECT FROM Owner)";
      case ROW -> "(SELECT count(*) AS c FROM Owner)";
      case NEW_ELEMENT -> "(SELECT expand(emb) FROM Holder)";
    };
  }

  private void assertStoredSubquery(final Kind kind, final String source, final int expectedEntries) {
    int rows = 0;
    try (final ResultSet rs = database.query("sql", "SELECT sub FROM " + source)) {
      while (rs.hasNext()) {
        final List<?> sub = rs.next().getProperty("sub");
        assertThat(sub).hasSize(expectedEntries);
        if (kind == Kind.ROW)
          assertThat(((Map<?, ?>) sub.getFirst()).get("c")).isEqualTo(1L);
        else if (kind == Kind.RECORD)
          assertThat(((Identifiable) sub.getFirst()).getIdentity()).isEqualTo(savedRid);
        else
          assertThat(sub).allSatisfy(e -> assertThat(e).isInstanceOf(Document.class));
        ++rows;
      }
    }
    assertThat(rows).isGreaterThan(0);
  }

  private int entriesOf(final Kind kind) {
    return kind == Kind.NEW_ELEMENT ? 3 : 1;
  }

  @ParameterizedTest
  @EnumSource(Kind.class)
  void insertFromAProjectedSubquery(final Kind kind) {
    database.getSchema().createDocumentType("T");
    database.transaction(() -> database.command("sql", "INSERT INTO T FROM SELECT id, " + subqueryOf(kind) + " AS sub FROM Base").close());

    assertStoredSubquery(kind, "T", entriesOf(kind));
  }

  // a single-column row assigned with SET is the column's value, not a row (that is the SET rule, not the serializer's), so
  // the ROW kind is covered by the three routes above
  @ParameterizedTest
  @EnumSource(value = Kind.class, names = { "RECORD", "NEW_ELEMENT" })
  void insertSetOfASubquery(final Kind kind) {
    database.getSchema().createDocumentType("T");
    database.transaction(() -> database.command("sql", "INSERT INTO T SET sub = " + subqueryOf(kind)).close());

    assertStoredSubquery(kind, "T", entriesOf(kind));
  }

  @ParameterizedTest
  @ValueSource(strings = { "", " REFRESH INCREMENTAL" })
  void materializedViewOfAProjectedSubquery(final String refresh) {
    for (final Kind kind : Kind.values()) {
      final String view = "mv" + kind.ordinal();
      database.command("sql", "CREATE MATERIALIZED VIEW " + view + " AS SELECT id, " + subqueryOf(kind) + " AS sub FROM Base" + refresh)
          .close();
      assertStoredSubquery(kind, view, entriesOf(kind));
    }
  }
}
