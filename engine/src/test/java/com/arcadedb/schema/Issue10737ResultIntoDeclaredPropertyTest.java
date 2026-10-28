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

import com.arcadedb.TestHelper;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10737: #10607 unwrapped a query {@link Result} in the serializer, which sits below the schema conversion, so a
 * {@code Result} assigned to a DECLARED property met {@code Type.convert} still wrapped and was refused (a {@code LINK} with
 * an {@code IllegalArgumentException}, an {@code EMBEDDED} with a {@code ValidationException}) where an undeclared property
 * stored it. The unwrap now happens where the value enters the record, before the property is looked up.
 * <p>
 * The census runs over every {@link Type} of the enum, so a new type cannot be added without the pair being tested, and
 * carries the dimension the defect lives in: declared against undeclared. Its invariant is that a {@code Result} behaves
 * exactly as what it stands for: for each kind of {@code Result}, as the value or inside a list or a map, the declared
 * property of every type and an undeclared one store the {@code Result} and the unwrapped value alike - both stored and
 * read back equal, or both refused with the same exception.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10737ResultIntoDeclaredPropertyTest extends TestHelper {
  private static final String SINK      = "Census";
  private static final String UNDECLARED = "undeclared";

  private enum Kind {
    /** a record with an identity */
    RECORD,
    /** a row that is not a record */
    ROW,
    /** an element nobody saved */
    NEW_ELEMENT
  }

  private enum Shape {
    VALUE, IN_LIST, IN_MAP
  }

  private RID savedRid;

  @BeforeEach
  void schema() {
    database.getSchema().createDocumentType("Emb");
    database.getSchema().createDocumentType("Owner");
    final DocumentType sink = database.getSchema().createDocumentType(SINK);
    for (final Type type : Type.values())
      sink.createProperty(declared(type), type);
    database.transaction(() -> savedRid = database.newDocument("Owner").set("k", 7).save().getIdentity());
  }

  @ParameterizedTest(name = "{0}")
  @EnumSource(Type.class)
  void aDeclaredPropertyTakesAResultAsWhatItStandsFor(final Type type) {
    for (final Kind kind : Kind.values())
      for (final Shape shape : Shape.values()) {
        final String what = type + " <- " + kind + " " + shape;
        assertThat(store(declared(type), shape, k -> resultOf(kind))).as(what)
            .isEqualTo(store(declared(type), shape, k -> unwrapped(kind)));
      }
  }

  @Test
  void anUndeclaredPropertyTakesAResultAsWhatItStandsFor() {
    for (final Kind kind : Kind.values())
      for (final Shape shape : Shape.values())
        assertThat(store(UNDECLARED, shape, k -> resultOf(kind))).as(kind + " " + shape)
            .isEqualTo(store(UNDECLARED, shape, k -> unwrapped(kind)))
            // THE UNDECLARED CONTROL OF #10607: EVERY KIND IS STORED THERE
            .startsWith("stored ");
  }

  /** The reporter's two rows: a declared LINK and a declared EMBEDDED take the Result they used to refuse. */
  @Test
  void theReportedDeclaredLinkAndEmbeddedAreStored() {
    assertThat(store(declared(Type.LINK), Shape.VALUE, k -> resultOf(Kind.RECORD))).isEqualTo("stored " + savedRid);
    assertThat(store(declared(Type.EMBEDDED), Shape.VALUE, k -> resultOf(Kind.NEW_ELEMENT))).isEqualTo("stored {x=1}");
  }

  /**
   * A GraphBatch edge stores a Result as the record it stands for, in a declared LINK property (through the schema
   * conversion) and in an undeclared one (through the template writer, which used to cast the Result to an Identifiable).
   */
  @Test
  void aGraphBatchEdgeStoresAResult() {
    database.getSchema().createVertexType("V");
    database.getSchema().createEdgeType("Plain");
    database.getSchema().createEdgeType("Typed").createProperty("target", Type.LINK);

    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      vertices[0] = database.newVertex("V").save().getIdentity();
      vertices[1] = database.newVertex("V").save().getIdentity();
    });

    try (final GraphBatch batch = database.batch().build()) {
      batch.newEdge(vertices[0], "Plain", vertices[1], "target", resultOf(Kind.RECORD));
      batch.newEdge(vertices[0], "Typed", vertices[1], "target", resultOf(Kind.RECORD));
    }

    for (final String edgeType : List.of("Plain", "Typed"))
      try (final ResultSet rs = database.query("sql", "SELECT target FROM " + edgeType)) {
        assertThat(((Identifiable) rs.next().getProperty("target")).getIdentity()).as(edgeType).isEqualTo(savedRid);
      }
    assertThat(((Vertex) vertices[0].asVertex()).countEdges(Vertex.DIRECTION.OUT, null)).isEqualTo(2);
  }

  private static String declared(final Type type) {
    return "p_" + type.name();
  }

  private Result resultOf(final Kind kind) {
    return switch (kind) {
      case RECORD -> new ResultInternal(savedRid.asDocument());
      case ROW -> new ResultInternal(new HashMap<>(Map.of("x", 1)));
      case NEW_ELEMENT -> new ResultInternal(newElement());
    };
  }

  private Object unwrapped(final Kind kind) {
    return switch (kind) {
      case RECORD -> savedRid.asDocument();
      case ROW -> new HashMap<>(Map.of("x", 1));
      case NEW_ELEMENT -> newElement();
    };
  }

  private MutableEmbeddedDocument newElement() {
    final MutableEmbeddedDocument element = database.newDocument(SINK).newEmbeddedDocument("Emb", "tmp");
    element.set("x", 1);
    return element;
  }

  /**
   * Stores the value {@code factory} builds, shaped by {@code shape}, into {@code property} of a new record, and answers what
   * it reads back, or the exception that refused it.
   */
  private String store(final String property, final Shape shape, final Function<Kind, Object> factory) {
    try {
      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final Object value = factory.apply(null);
        final Object shaped = switch (shape) {
          case VALUE -> value;
          case IN_LIST -> new ArrayList<>(List.of(value));
          case IN_MAP -> new LinkedHashMap<>(Map.of("k", value));
        };
        final MutableDocument doc = database.newDocument(SINK);
        doc.set(property, shaped);
        rid[0] = doc.save().getIdentity();
      });
      return "stored " + normalize(rid[0].asDocument().get(property));
    } catch (final Exception e) {
      return "refused " + e.getClass().getSimpleName();
    } finally {
      if (database.isTransactionActive())
        database.rollback();
    }
  }

  /** A comparable form of a stored value: a record by its identity, an embedded document by its properties. */
  private static Object normalize(final Object value) {
    if (value instanceof Document document && document.getIdentity() == null)
      return normalize(document.toMap(false));
    if (value instanceof Identifiable identifiable)
      return identifiable.getIdentity();
    if (value instanceof Map<?, ?> map) {
      final Map<String, Object> sorted = new TreeMap<>();
      for (final Map.Entry<?, ?> entry : map.entrySet())
        sorted.put(String.valueOf(entry.getKey()), normalize(entry.getValue()));
      return sorted;
    }
    if (value instanceof List<?> list) {
      final List<Object> normalized = new ArrayList<>(list.size());
      for (final Object item : list)
        normalized.add(normalize(item));
      return normalized;
    }
    return value;
  }
}
