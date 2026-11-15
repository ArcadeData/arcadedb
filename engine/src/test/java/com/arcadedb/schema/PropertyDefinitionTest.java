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
import com.arcadedb.database.EmbeddedDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.GraphBatch;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.CreatePropertyAttributeStatement;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class PropertyDefinitionTest {

  /**
   * Regression for #10099: {@code CREATE PROPERTY T.a STRING (regexp null)} stored the four-character pattern {@code null}, so from then on
   * the only value the property could hold was the text {@code null}, and the state survived a reopen without a warning, while
   * {@code ALTER PROPERTY T.a REGEXP null} cleared the pattern. The two statements set the same attributes and disagreed on what
   * the word {@code null} means.
   * <p>
   * The guarantee is the family, not the reported member: for every attribute {@code CREATE PROPERTY ... ( ... )} accepts, the SQL
   * literal {@code null} produces what {@code ALTER PROPERTY ... <attribute> null} produces. The attribute list is the one the
   * statement itself publishes, so an attribute added later is under this check by construction.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CreatePropertyNullAttribute extends TestHelper {

    @Test
    void createPropertyRegexpNullDoesNotStoreTheTextNull() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING (regexp null)");

      assertThat(database.getSchema().getType("T").getProperty("a").getRegexp()).isNull();
      database.transaction(() -> database.command("sql", "INSERT INTO T SET a = 'hello'"));

      reopenDatabase();

      assertThat(database.getSchema().getType("T").getProperty("a").getRegexp()).as("the pattern survives no reopen, there is none")
          .isNull();
      database.transaction(() -> database.command("sql", "INSERT INTO T SET a = 'again'"));
      assertThat(database.countType("T", false)).isEqualTo(2);
    }

    @Test
    void aQuotedNullIsAPatternThatTheCallerAskedFor() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING (regexp 'null')");
      assertThat(database.getSchema().getType("T").getProperty("a").getRegexp()).isEqualTo("null");

      database.command("sql", "CREATE DOCUMENT TYPE U");
      database.command("sql", "CREATE PROPERTY U.a STRING");
      database.command("sql", "ALTER PROPERTY U.a REGEXP 'null'");
      assertThat(database.getSchema().getType("U").getProperty("a").getRegexp()).isEqualTo("null");
    }

    @Test
    void minAndMaxNullAreAcceptedAndLeaveNoBound() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.b LONG (min null, max null)");

      final Property property = database.getSchema().getType("T").getProperty("b");
      assertThat(property.getMin()).isNull();
      assertThat(property.getMax()).isNull();
      database.transaction(() -> database.command("sql", "INSERT INTO T SET b = 123456789"));
    }

    @Test
    void compressionNullIsNoCompression() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING (compression null)");
      assertThat(database.getSchema().getType("T").getProperty("a").getCompression()).isIn(null, "none");

      database.command("sql", "ALTER PROPERTY T.a COMPRESSION 'fast'");
      assertThat(database.getSchema().getType("T").getProperty("a").getCompression()).isEqualTo("fast");
      database.command("sql", "ALTER PROPERTY T.a COMPRESSION null");
      assertThat(database.getSchema().getType("T").getProperty("a").getCompression()).isIn(null, "none");
    }

    @Test
    void aBooleanAttributeRefusesNullWithAMessageFromBothStatements() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.a STRING");

      for (final String attribute : List.of("readonly", "mandatory", "notnull", "hidden", "external")) {
        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.b" + attribute + " STRING (" + attribute + " null)")).as(
            "CREATE " + attribute).isInstanceOf(CommandExecutionException.class).hasMessageContaining("true or false");
        assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.a " + attribute + " null")).as("ALTER " + attribute)
            .isInstanceOf(CommandExecutionException.class).hasMessageContaining("true or false");
        assertThat(database.getSchema().getType("T").existsProperty("b" + attribute)).as("the refused CREATE leaves nothing")
            .isFalse();
      }
    }

    @Test
    void aBareWordIsAFieldReferenceNotANullAndIsRefusedByBothStatements() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.s STRING (regexp '[a-z]+')");
      database.command("sql", "CREATE PROPERTY T.n LONG (min 1, max 9)");

      // ALTER used to read the unresolved word as null, which silently cleared the pattern or the bound it was meant to set
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s REGEXP abc")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("field reference").hasMessageContaining("'abc'");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.n MAX nine")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("field reference");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.n MIN one")).isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("field reference");
      assertThatThrownBy(() -> database.command("sql", "ALTER PROPERTY T.s COMPRESSION fast")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("field reference");

      final Property s = database.getSchema().getType("T").getProperty("s");
      final Property n = database.getSchema().getType("T").getProperty("n");
      assertThat(s.getRegexp()).isEqualTo("[a-z]+");
      assertThat(n.getMax()).isEqualTo("9");
      assertThat(n.getMin()).isEqualTo("1");

      // and CREATE used to store the unresolved word as the text null
      for (final String attribute : List.of("regexp abc", "compression fast"))
        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.x STRING (" + attribute + ")")).as(attribute)
            .isInstanceOf(CommandExecutionException.class).hasMessageContaining("field reference");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.y LONG (max nine)")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("field reference");
      assertThat(database.getSchema().getType("T").existsProperty("x")).isFalse();
      assertThat(database.getSchema().getType("T").existsProperty("y")).isFalse();
    }

    @Test
    void nullMeansTheSameForCreateAndAlterOnEveryAttributeCreateAccepts() {
      final List<String> disagreements = new ArrayList<>();
      int index = 0;
      for (final String attribute : CreatePropertyAttributeStatement.ATTRIBUTES) {
        final String propertyType = attribute.equals("min") || attribute.equals("max") ? "LONG" : "STRING";
        final String created = "C" + index;
        final String altered = "A" + index++;

        database.command("sql", "CREATE DOCUMENT TYPE " + created);
        database.command("sql", "CREATE DOCUMENT TYPE " + altered);
        database.command("sql", "CREATE PROPERTY " + altered + ".p " + propertyType);

        final String fromCreate = outcome(created, "CREATE PROPERTY " + created + ".p " + propertyType + " (" + attribute + " null)");
        final String fromAlter = outcome(altered, "ALTER PROPERTY " + altered + ".p " + attribute + " null");

        if (!fromCreate.equals(fromAlter))
          disagreements.add(attribute + ": CREATE " + fromCreate + " but ALTER " + fromAlter);
      }
      assertThat(disagreements).as("CREATE PROPERTY and ALTER PROPERTY disagree on the SQL literal null").isEmpty();
    }

    @Test
    void theListedAttributesAreTheOnesCreateAccepts() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.a STRING (nosuchattribute 1)")).isInstanceOf(
          CommandExecutionException.class).hasMessageContaining("Invalid attribute definition");

      // every listed attribute is accepted by CREATE with a value of its own kind
      int index = 0;
      for (final String attribute : CreatePropertyAttributeStatement.ATTRIBUTES) {
        final String propertyType = attribute.equals("min") || attribute.equals("max") ? "LONG" : "STRING";
        final String value = switch (attribute) {
          case "compression" -> "'fast'";
          case "min", "max" -> "5";
          case "default", "regexp" -> "'x'";
          default -> "true";
        };
        final String name = "p" + index++;
        database.command("sql", "CREATE PROPERTY T." + name + " " + propertyType + " (" + attribute + " " + value + ")");
        assertThat(database.getSchema().getType("T").existsProperty(name)).as(attribute).isTrue();
      }
    }

    /** What the statement left behind: either that it was refused, or every attribute the property carries. */
    private String outcome(final String typeName, final String sql) {
      try {
        database.command("sql", sql);
      } catch (final RuntimeException e) {
        return "REFUSED";
      }
      final Property p = database.getSchema().getType(typeName).getProperty("p");
      return "readonly=" + p.isReadonly() + " mandatory=" + p.isMandatory() + " notnull=" + p.isNotNull() + " hidden=" + p.isHidden()
          + " external=" + p.isExternal() + " compression=" + bracket(p.getCompression()) + " max=" + bracket(p.getMax()) + " min="
          + bracket(p.getMin()) + " default=" + bracket(p.getDefaultValueDefinition()) + " regexp=" + bracket(p.getRegexp());
    }

    /** The text {@code null} and no value at all must not look alike. */
    private static String bracket(final Object value) {
      return value == null ? "<none>" : "[" + value + "]";
    }
  }

  /**
   * Regression for #10194, second point: an explicit NULL for a property that has a default stored the default. In SQL the default fills a column
   * that is OMITTED from the statement, and an explicit NULL is stored as NULL. {@code UPDATE ... APPLY DEFAULTS}, whose job is to
   * fill what is null, keeps doing so.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class ExplicitNullKeepsNull extends TestHelper {

    @BeforeEach
    void createType() {
      database.command("sql", "CREATE DOCUMENT TYPE T").close();
      database.command("sql", "CREATE PROPERTY T.id LONG").close();
      database.command("sql", "CREATE PROPERTY T.df INTEGER (default 7)").close();
    }

    private Document only() {
      try (final ResultSet rs = database.query("sql", "SELECT FROM T")) {
        final Document doc = rs.next().getElement().orElseThrow();
        assertThat(rs.hasNext()).isFalse();
        return doc;
      }
    }

    private void run(final String statement) {
      database.transaction(() -> database.command("sql", statement).close());
    }

    @Test
    void insertSetWithExplicitNullStoresNull() {
      run("INSERT INTO T SET id = 1, df = null");

      final Document doc = only();
      assertThat(doc.has("df")).isTrue();
      assertThat(doc.get("df")).isNull();
    }

    @Test
    void insertSetWithTheColumnOmittedStoresTheDefault() {
      run("INSERT INTO T SET id = 1");

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void insertValuesWithExplicitNullStoresNull() {
      run("INSERT INTO T (id, df) VALUES (1, null)");

      final Document doc = only();
      assertThat(doc.has("df")).isTrue();
      assertThat(doc.get("df")).isNull();
    }

    @Test
    void insertValuesWithTheColumnOmittedStoresTheDefault() {
      run("INSERT INTO T (id) VALUES (1)");

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void insertContentWithExplicitNullStoresNull() {
      run("INSERT INTO T CONTENT {\"id\": 1, \"df\": null}");

      final Document doc = only();
      assertThat(doc.has("df")).isTrue();
      assertThat(doc.get("df")).isNull();
    }

    @Test
    void insertContentWithTheKeyOmittedStoresTheDefault() {
      run("INSERT INTO T CONTENT {\"id\": 1}");

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void insertFromSelectOfANullStoresNull() {
      database.command("sql", "CREATE DOCUMENT TYPE Src").close();
      database.transaction(() -> database.newDocument("Src").set("id", 1L).set("df", null).save());

      run("INSERT INTO T FROM SELECT id, df FROM Src");

      final Document doc = only();
      assertThat(doc.has("df")).isTrue();
      assertThat(doc.get("df")).isNull();
    }

    @Test
    void javaApiExplicitNullStoresNull() {
      database.transaction(() -> database.newDocument("T").set("id", 1L).set("df", null).save());

      final Document doc = only();
      assertThat(doc.has("df")).isTrue();
      assertThat(doc.get("df")).isNull();
    }

    @Test
    void javaApiOmittedPropertyStoresTheDefault() {
      database.transaction(() -> database.newDocument("T").set("id", 1L).save());

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void anExplicitNullSurvivesTheReload() {
      run("INSERT INTO T SET id = 1, df = null");

      final Document reloaded = database.lookupByRID(only().getIdentity(), true).asDocument();
      assertThat(reloaded.has("df")).isTrue();
      assertThat(reloaded.get("df")).isNull();
    }

    @Test
    void updateSetToNullStaysNull() {
      run("INSERT INTO T SET id = 1");
      run("UPDATE T SET df = null");

      assertThat(only().get("df")).isNull();
    }

    @Test
    void applyDefaultsStillFillsAnExplicitNull() {
      run("INSERT INTO T SET id = 1, df = null");

      run("UPDATE T SET id = 2 APPLY DEFAULTS");

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void notNullPropertyWithADefaultRefusesAnExplicitNull() {
      database.command("sql", "ALTER PROPERTY T.df NOTNULL true").close();

      assertThatThrownBy(() -> run("INSERT INTO T SET id = 1, df = null")).isInstanceOf(ValidationException.class);
      run("INSERT INTO T SET id = 2");

      assertThat(only().getInteger("df")).isEqualTo(7);
    }

    @Test
    void aRefusedExplicitNullInsideACallerKeptTransactionLeavesItUsable() {
      database.command("sql", "ALTER PROPERTY T.df NOTNULL true").close();

      database.transaction(() -> {
        database.command("sql", "INSERT INTO T SET id = 1").close();
        assertThatThrownBy(() -> database.command("sql", "INSERT INTO T SET id = 2, df = null").close())
            .isInstanceOf(ValidationException.class);
        database.command("sql", "INSERT INTO T SET id = 3").close();
      });

      try (final ResultSet rs = database.query("sql", "SELECT id, df FROM T ORDER BY id")) {
        assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(1L);
        assertThat(rs.next().<Number>getProperty("id").longValue()).isEqualTo(3L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void edgeCreatedWithAnExplicitNullKeepsNull() {
      database.command("sql", "CREATE VERTEX TYPE V").close();
      database.command("sql", "CREATE EDGE TYPE E").close();
      database.command("sql", "CREATE PROPERTY E.w INTEGER (default 5)").close();
      database.transaction(() -> {
        database.command("sql", "CREATE VERTEX V").close();
        database.command("sql", "CREATE VERTEX V").close();
      });

      run("CREATE EDGE E FROM (SELECT FROM V LIMIT 1) TO (SELECT FROM V SKIP 1 LIMIT 1) SET w = null");
      run("CREATE EDGE E FROM (SELECT FROM V SKIP 1 LIMIT 1) TO (SELECT FROM V LIMIT 1)");

      int nulls = 0;
      int defaults = 0;
      try (final ResultSet rs = database.query("sql", "SELECT FROM E")) {
        while (rs.hasNext()) {
          final Document edge = rs.next().getElement().orElseThrow();
          if (edge.get("w") == null)
            ++nulls;
          else if (edge.getInteger("w") == 5)
            ++defaults;
        }
      }
      assertThat(nulls).isEqualTo(1);
      assertThat(defaults).isEqualTo(1);
    }
  }

  /**
   * Regression for #10204: {@code CREATE PROPERTY T.n STRING} was accepted over records holding {@code n} as integers, without converting
   * them, so an index created on {@code n} afterwards compared as text what a scan compared as numbers: the range
   * {@code n >= 20 AND n < 100} answered 0 rows through the index and 80 by a scan. Every other declared type is held to the
   * stored values ({@code CREATE PROPERTY U.s INTEGER} over {@code 'abc'} is refused): STRING now is too.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class StringPropertyOverStoredNumbers extends TestHelper {

    @Test
    void stringOverStoredIntegersIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.transaction(() -> {
        for (int i = 1; i <= 200; i++)
          database.newDocument("T").set("id", i, "n", i).save();
      });

      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY T.n STRING"))
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("Cannot create property 'T.n'")
          .hasMessageContaining("STRING")
          .hasMessageContaining("Integer");
      assertThat(database.getSchema().getType("T").existsProperty("n")).isFalse();

      // CONVERTED FIRST, THE DECLARATION IS ACCEPTED AND THE INDEX AGREES WITH A SCAN
      database.transaction(() -> database.command("sql", "UPDATE T SET n = n.asString()"));
      database.command("sql", "CREATE PROPERTY T.n STRING");
      database.command("sql", "CREATE INDEX ON T (n) NOTUNIQUE");
      assertThat(count("SELECT count(*) AS c FROM T WHERE n = '150'")).isEqualTo(1L);
      assertThat(count("SELECT count(*) AS c FROM T WHERE n >= '20' AND n < '30'"))
          .isEqualTo(count("SELECT count(*) AS c FROM T WHERE n.asString() >= '20' AND n.asString() < '30'"));
    }

    @Test
    void stringOverOtherStoredScalarsIsRefused() {
      final Object[] values = { 3L, (short) 7, 1.5f, 2.5, true, LocalDate.of(2026, 1, 2) };
      for (int i = 0; i < values.length; i++) {
        final String typeName = "S" + i;
        final Object value = values[i];
        database.command("sql", "CREATE DOCUMENT TYPE " + typeName);
        database.transaction(() -> database.newDocument(typeName).set("n", value).save());

        assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY " + typeName + ".n STRING"))
            .as(value.getClass().getSimpleName())
            .isInstanceOf(CommandExecutionException.class)
            .hasMessageContaining("STRING");
        assertThat(database.getSchema().getType(typeName).existsProperty("n")).isFalse();
      }
    }

    @Test
    void stringOverStoredStringsAndNullsIsAccepted() {
      database.command("sql", "CREATE DOCUMENT TYPE W");
      database.transaction(() -> {
        database.newDocument("W").set("n", "abc").save();
        database.newDocument("W").set("n", (Object) null).save();
        database.newDocument("W").set("other", 5).save();
      });

      database.command("sql", "CREATE PROPERTY W.n STRING");
      assertThat(database.getSchema().getType("W").getProperty("n").getType()).isEqualTo(Type.STRING);
    }

    @Test
    void aSubtypeRecordIsCheckedToo() {
      database.command("sql", "CREATE DOCUMENT TYPE P");
      database.command("sql", "CREATE DOCUMENT TYPE C EXTENDS P");
      database.transaction(() -> {
        database.newDocument("P").set("n", "a").save();
        database.newDocument("C").set("n", 42).save();
      });

      assertThatThrownBy(() -> database.command("sql", "CREATE PROPERTY P.n STRING")).isInstanceOf(CommandExecutionException.class);
    }

    private long count(final String sql) {
      try (final ResultSet rs = database.query("sql", sql)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }
  }

  /**
   * Regression for #10737: #10607 unwrapped a query {@link Result} in the serializer, which sits below the schema conversion, so a
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
  @Nested
  class ResultIntoDeclaredProperty extends TestHelper {
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

  /**
   * Regression for #10488: the HTTP command handler parses the request with {@code toMap(true)}, which turns a
   * homogeneous numeric array found anywhere in the tree into a primitive {@code long[]}/{@code double[]}. The top level of a
   * parameter is turned back into a list before it is bound, but a list nested in a list or in a map was stored as the
   * primitive array it was parsed to, so {@code =}, {@code .type()} and {@code GROUP BY} saw a different value than for the
   * same list written as a literal or from Java.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NestedNumericArrayParam extends TestHelper {

    private static final String REQUEST = "{\"params\":{\"m\":{\"a\":[1,2]},\"ll\":[[1,2],[3]],\"md\":{\"a\":[1.5,2.5]}}}";

    @Test
    void nestedListParameterIsStoredAsAList() {
      createType();
      insertRows();

      try (final ResultSet rs = database.query("sql", "SELECT FROM D ORDER BY k")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          final String k = row.getProperty("k");
          assertThat(((Map<?, ?>) row.getProperty("m")).get("a")).as("m.a of " + k).isInstanceOf(List.class);
          assertThat(((List<?>) row.getProperty("ll")).get(0)).as("ll[0] of " + k).isInstanceOf(List.class);
          assertThat(((Map<?, ?>) row.getProperty("md")).get("a")).as("md.a of " + k).isInstanceOf(List.class);
        }
      }
    }

    @Test
    void nestedListParameterAnswersLikeTheLiteralAndTheJavaValue() {
      createType();
      insertRows();

      assertThat(keys("SELECT k FROM D WHERE m.a = [1, 2] ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE ll[0] = [1, 2] ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE m.a CONTAINS 2 ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE m.a.size() = 2 ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE m.a[1] = 2 ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE ll[0].size() = 2 ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE md.a = [1.5, 2.5] ORDER BY k")).containsExactly("java", "literal", "param");
    }

    @Test
    void identicalMapsFromParameterLiteralAndJavaFallInOneGroup() {
      createType();
      insertRows();

      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM D GROUP BY m")) {
        final List<Long> counts = new ArrayList<>();
        while (rs.hasNext())
          counts.add(rs.next().<Long>getProperty("c"));
        assertThat(counts).containsExactly(3L);
      }
    }

    @Test
    void nestedListIsStoredAsAListAfterAReopen() {
      createType();
      insertRows();
      reopenDatabase();

      assertThat(keys("SELECT k FROM D WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("java", "literal", "param");
      assertThat(keys("SELECT k FROM D WHERE m.a = [1, 2] ORDER BY k")).containsExactly("java", "literal", "param");
    }

    @Test
    void aTopLevelPrimitiveArrayOfAListPropertyStillBecomesAList() {
      database.command("sql", "CREATE DOCUMENT TYPE T");
      database.command("sql", "CREATE PROPERTY T.l LIST");

      final Map<String, Object> params = new HashMap<>();
      params.put("l", new long[] { 1L, 2L });
      database.transaction(() -> database.command("sql", "INSERT INTO T SET l = :l", params).close());

      try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE l.type() = 'LIST'")) {
        assertThat(rs.stream().count()).isEqualTo(1L);
      }
    }

    @Test
    void undeclaredPropertyStoresTheNestedListAsAList() {
      database.command("sql", "CREATE DOCUMENT TYPE Schemaless");
      final Map<String, Object> params = new HashMap<>();
      params.put("m", Map.of("a", new long[] { 1L, 2L }));
      params.put("l", List.of(new double[] { 1.5, 2.5 }));
      database.transaction(() -> database.command("sql", "INSERT INTO Schemaless SET m = :m, l = :l", params).close());

      try (final ResultSet rs = database.query("sql", "SELECT FROM Schemaless WHERE m.a = [1, 2] AND l[0] = [1.5, 2.5]")) {
        assertThat(rs.stream().count()).isEqualTo(1L);
      }
    }

    @Test
    void unwrapKeepsTheContainerWhenThereIsNothingToReplace() {
      final Map<String, Object> plain = Map.of("a", List.of(1, 2), "b", Map.of("c", "x"));
      assertThat(Type.unwrapNestedPrimitiveArrays(plain)).isSameAs(plain);

      final List<Object> list = List.of(List.of(1), 2, "x");
      assertThat(Type.unwrapNestedPrimitiveArrays(list)).isSameAs(list);

      final long[] topLevel = { 1L, 2L };
      assertThat(Type.unwrapNestedPrimitiveArrays(topLevel)).isSameAs(topLevel);
      assertThat(Type.unwrapNestedPrimitiveArrays(null)).isNull();
      assertThat(Type.unwrapNestedPrimitiveArrays("text")).isEqualTo("text");
    }

    @Test
    void unwrapReplacesAtEveryDepthAndNarrowsLikeTheJsonParser() {
      final Map<String, Object> nested = new HashMap<>();
      nested.put("deep", List.of(Map.of("a", new long[] { 1L, 5_000_000_000L }), new double[] { 0.5 }));

      @SuppressWarnings("unchecked")
      final Map<String, Object> unwrapped = (Map<String, Object>) Type.unwrapNestedPrimitiveArrays(nested);

      assertThat(unwrapped).isNotSameAs(nested);
      assertThat(unwrapped.get("deep")).isEqualTo(List.of(Map.of("a", List.of(1, 5_000_000_000L)), List.of(0.5)));
      // the source is not touched
      assertThat(((List<?>) nested.get("deep")).get(1)).isInstanceOf(double[].class);
    }

    /**
     * The unwrapper replaces {@code long[]} and {@code double[]} and nothing else, because those are the only arrays the request
     * parser leaves in a value. This ties the two: a primitive array of any other kind showing up in a parsed request has to be
     * added to {@code Type.unwrapNestedPrimitiveArrays} too.
     */
    @Test
    void theRequestParserLeavesNoPrimitiveArrayTheUnwrapperDoesNotReplace() {
      final Map<String, Object> parsed = new JSONObject(
          "{\"a\":[1,2],\"b\":[1.5],\"c\":[1,2.5],\"d\":[[1],[2.5],[\"x\"]],\"e\":[\"x\"],\"f\":[true],\"g\":[1e3],"
              + "\"h\":[12345678901234567890],\"i\":{\"j\":[[1,2],[3]],\"k\":[0.5,0.25]},\"l\":[],\"m\":[null,1],\"n\":[[]]}")
          .toMap(true);

      assertThat(Type.unwrapNestedPrimitiveArrays(parsed)).isNotNull();
      assertThat(primitiveArraysIn(Type.unwrapNestedPrimitiveArrays(parsed))).as("arrays left after the unwrap").isEmpty();
      assertThat(primitiveArraysIn(parsed)).as("arrays the parser leaves").allMatch(
          array -> array == long[].class || array == double[].class);
    }

    private static List<Class<?>> primitiveArraysIn(final Object value) {
      final List<Class<?>> found = new ArrayList<>();
      if (value != null && value.getClass().isArray())
        found.add(value.getClass());
      else if (value instanceof Map<?, ?> map)
        for (final Object item : map.values())
          found.addAll(primitiveArraysIn(item));
      else if (value instanceof List<?> list)
        for (final Object item : list)
          found.addAll(primitiveArraysIn(item));
      return found;
    }

    /** A list holding an integer beyond the int range: the elements are an Integer and a Long, as the literal's are. */
    @Test
    void aNestedListWithALongBeyondTheIntRangeEqualsTheLiteralAndGroupsWithIt() {
      database.command("sql", "CREATE DOCUMENT TYPE Big");
      final Map<String, Object> request = new JSONObject("{\"params\":{\"m\":{\"a\":[5000000000,1]}}}").toMap(true);
      @SuppressWarnings("unchecked")
      final Map<String, Object> params = (Map<String, Object>) request.get("params");

      database.transaction(() -> {
        database.command("sql", "INSERT INTO Big SET k = 'param', m = :m", params).close();
        database.command("sql", "INSERT INTO Big SET k = 'literal', m = {\"a\": [5000000000, 1]}").close();
      });

      assertThat(keys("SELECT k FROM Big WHERE m.a = [5000000000, 1] ORDER BY k")).containsExactly("literal", "param");
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM Big GROUP BY m")) {
        assertThat(rs.next().<Long>getProperty("c")).isEqualTo(2L);
        assertThat(rs.hasNext()).isFalse();
      }
    }

    /** Every way a map or a list reaches a record funnels through the one place that unwraps: the setters, the map and JSON loads, MERGE and CONTENT. */
    @Test
    void everyPathThatAssignsAPropertyStoresTheNestedListAsAList() {
      database.command("sql", "CREATE DOCUMENT TYPE Paths");
      final Map<String, Object> nested = new HashMap<>();
      nested.put("a", new long[] { 1L, 2L });

      final Map<String, Object> contentRow = new HashMap<>();
      contentRow.put("k", "content");
      contentRow.put("m", nested);
      final Map<String, Object> mergeParams = new HashMap<>();
      mergeParams.put("m", nested);

      database.transaction(() -> {
        database.newDocument("Paths").set("k", "set").set("m", nested).save();
        database.newDocument("Paths").set(Map.of("k", "setMap", "m", nested)).save();
        database.newDocument("Paths").fromMap(contentRow).set("k", "fromMap").save();
        database.newDocument("Paths").set("k", "merge").save();
        database.command("sql", "INSERT INTO Paths CONTENT :row", Map.of("row", contentRow)).close();
        database.command("sql", "UPDATE Paths MERGE {\"m\": {\"a\": [1, 2]}} WHERE k = 'merge'").close();
      });
      database.transaction(() -> {
        database.command("sql", "UPDATE Paths SET m = :m WHERE k = 'merge'", mergeParams).close();
        database.command("sql", "UPDATE Paths CONTENT :row WHERE k = 'setMap'", Map.of("row", Map.of("k", "setMap", "m", nested))).close();
      });

      assertThat(keys("SELECT k FROM Paths WHERE m.a.type() = 'LIST' ORDER BY k")).containsExactly("content", "fromMap", "merge", "set",
          "setMap");
      assertThat(keys("SELECT k FROM Paths WHERE m.a = [1, 2] ORDER BY k")).containsExactly("content", "fromMap", "merge", "set", "setMap");
    }

    /**
     * What a container becomes when it holds a replacement is documented here: a sorted or linked input is copied to the plain
     * family of its kind, and one that holds no array is returned as it came, whatever its type.
     */
    @Test
    void aContainerKeepsItsTypeUnlessItHoldsAReplacement() {
      final TreeMap<String, Object> sortedPlain = new TreeMap<>(Map.of("a", 1));
      final LinkedList<Object> linkedPlain = new LinkedList<>(List.of(1, 2));
      assertThat(Type.unwrapNestedPrimitiveArrays(sortedPlain)).isSameAs(sortedPlain);
      assertThat(Type.unwrapNestedPrimitiveArrays(linkedPlain)).isSameAs(linkedPlain);

      final TreeMap<String, Object> sortedWithArray = new TreeMap<>();
      sortedWithArray.put("a", new long[] { 1L });
      final Object unwrappedMap = Type.unwrapNestedPrimitiveArrays(sortedWithArray);
      assertThat(unwrappedMap).isInstanceOf(LinkedHashMap.class).isEqualTo(Map.of("a", List.of(1)));

      final LinkedList<Object> linkedWithArray = new LinkedList<>();
      linkedWithArray.add(new double[] { 0.5 });
      assertThat(Type.unwrapNestedPrimitiveArrays(linkedWithArray)).isInstanceOf(ArrayList.class).isEqualTo(List.of(List.of(0.5)));

      final TreeSet<Object> sortedSet = new TreeSet<>(List.of("b", "a"));
      assertThat(Type.unwrapNestedPrimitiveArrays(sortedSet)).isSameAs(sortedSet);
    }

    @Test
    void unwrapKeepsASetASet() {
      final Object unwrapped = Type.unwrapNestedPrimitiveArrays(new LinkedHashSet<Object>(List.of(new long[] { 1L })));
      assertThat(unwrapped).isInstanceOf(Set.class);
    }

    private void createType() {
      database.command("sql", "CREATE DOCUMENT TYPE D");
      database.command("sql", "CREATE PROPERTY D.m MAP");
      database.command("sql", "CREATE PROPERTY D.ll LIST");
      database.command("sql", "CREATE PROPERTY D.md MAP");
    }

    /** The three ways of writing the same values: the parameters as the HTTP handler parses them, a literal and the Java API. */
    @SuppressWarnings("unchecked")
    private void insertRows() {
      // PostCommandHandler parses the request body with toMap(true)
      final Map<String, Object> request = new JSONObject(REQUEST).toMap(true);
      final Map<String, Object> params = (Map<String, Object>) request.get("params");

      database.transaction(() -> {
        database.command("sql", "INSERT INTO D SET k = 'param', m = :m, ll = :ll, md = :md", params).close();
        database.command("sql", "INSERT INTO D SET k = 'literal', m = {\"a\": [1, 2]}, ll = [[1, 2], [3]], md = {\"a\": [1.5, 2.5]}").close();
        database.newDocument("D").set("k", "java").set("m", Map.of("a", List.of(1, 2)))
            .set("ll", List.of(List.of(1, 2), List.of(3))).set("md", Map.of("a", List.of(1.5, 2.5))).save();
      });
    }

    private List<String> keys(final String sql) {
      final List<String> keys = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          keys.add(rs.next().getProperty("k"));
      }
      return keys;
    }
  }

  /**
   * Regression for #5261: a property declared as {@code MAP OF LONG} (or {@code LIST OF LONG}) rejects nested {@code Integer}
   * values that reach the write path from JSON (e.g. the remote client re-serializes a full record with {@code UPDATE ... CONTENT}
   * and small {@code Long} values arrive as untyped JSON integers parsed as {@link Integer}). The engine must coerce the nested
   * values to the declared {@code ofType} before validation, mirroring the top-level scalar coercion.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class NestedLongCoercion extends TestHelper {

    @Test
    void mapOfLongCoercesNestedIntegerValues() {
      database.getSchema().createDocumentType("MapLong5261").createProperty("values", Type.MAP).setOfType("LONG");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("MapLong5261");

        final Map<String, Object> values = new LinkedHashMap<>();
        // Small values that fit the 32-bit range: this is exactly what JSON parsing produces as Integer.
        values.put("first", 1001);
        values.put("second", 2002);
        doc.set("values", values);

        // No exception must be thrown by validation.
        doc.save();

        final Map<String, Object> stored = (Map<String, Object>) doc.get("values");
        assertThat(stored.get("first")).isInstanceOf(Long.class).isEqualTo(1001L);
        assertThat(stored.get("second")).isInstanceOf(Long.class).isEqualTo(2002L);
      });
    }

    @Test
    void listOfLongCoercesNestedIntegerValues() {
      database.getSchema().createDocumentType("ListLong5261").createProperty("numbers", Type.LIST).setOfType("LONG");

      database.transaction(() -> {
        final MutableDocument doc = database.newDocument("ListLong5261");
        doc.set("numbers", List.of(10, 20, 30));
        doc.save();

        final List<Object> stored = (List<Object>) doc.get("numbers");
        for (final Object item : stored)
          assertThat(item).isInstanceOf(Long.class);
        assertThat(stored).containsExactly(10L, 20L, 30L);
      });
    }

    @Test
    void mapOfLongViaFromMapCoercesNestedIntegerValues() {
      database.getSchema().createDocumentType("MapLongFromMap5261").createProperty("values", Type.MAP).setOfType("LONG");

      database.transaction(() -> {
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("first", 1001);
        values.put("second", 2002);

        final Map<String, Object> record = new LinkedHashMap<>();
        record.put("values", values);

        final MutableDocument doc = database.newDocument("MapLongFromMap5261").fromMap(record);
        doc.save();

        final Map<String, Object> stored = (Map<String, Object>) doc.get("values");
        assertThat(stored.get("first")).isInstanceOf(Long.class).isEqualTo(1001L);
        assertThat(stored.get("second")).isInstanceOf(Long.class).isEqualTo(2002L);
      });
    }
  }

  /**
   * Regression for #6819: an {@code EMBEDDED} property that declares an {@code ofType} still rejected a plain
   * {@link Map}, because the embedded type name was taken exclusively from the map's own {@code "@type"} entry even
   * though the schema already pinned it. The failure surfaced as {@code "Type with name 'null' was not found"} wrapped
   * in a generic convert error that named neither the missing key nor the declared {@code ofType}.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EmbeddedOfTypeFromMap extends TestHelper {

    @Test
    void plainMapUsesTheDeclaredOfType() {
      database.getSchema().createDocumentType("Address6819").createProperty("city", Type.STRING);
      database.getSchema().createDocumentType("Person6819").createProperty("address", Type.EMBEDDED).setOfType("Address6819");

      database.transaction(() -> {
        final MutableDocument person = database.newDocument("Person6819").set("address", Map.of("city", "Rome"));
        person.save();

        final EmbeddedDocument address = person.getEmbedded("address");
        assertThat(address).isNotNull();
        assertThat(address.getTypeName()).isEqualTo("Address6819");
        assertThat(address.getString("city")).isEqualTo("Rome");
      });

      final EmbeddedDocument reloaded = database.query("sql", "select from Person6819").next().getElement().get()
          .getEmbedded("address");
      assertThat(reloaded.getTypeName()).isEqualTo("Address6819");
      assertThat(reloaded.getString("city")).isEqualTo("Rome");
    }

    @Test
    void explicitTypeStillWinsOverTheDeclaredOfTypeWhenCompatible() {
      database.getSchema().createDocumentType("Address6819b").createProperty("city", Type.STRING);
      database.getSchema().createDocumentType("HomeAddress6819b").addSuperType("Address6819b").createProperty("floor", Type.INTEGER);
      database.getSchema().createDocumentType("Person6819b").createProperty("address", Type.EMBEDDED).setOfType("Address6819b");

      database.transaction(() -> {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("@type", "HomeAddress6819b");
        map.put("city", "Rome");
        map.put("floor", 3);

        final MutableDocument person = database.newDocument("Person6819b").set("address", map);
        person.save();

        final EmbeddedDocument address = person.getEmbedded("address");
        assertThat(address.getTypeName()).isEqualTo("HomeAddress6819b");
        assertThat(address.getString("city")).isEqualTo("Rome");
        assertThat(address.getInteger("floor")).isEqualTo(3);
      });
    }

    @Test
    void incompatibleExplicitTypeIsStillRejected() {
      database.getSchema().createDocumentType("Address6819c").createProperty("city", Type.STRING);
      database.getSchema().createDocumentType("Other6819c").createProperty("city", Type.STRING);
      database.getSchema().createDocumentType("Person6819c").createProperty("address", Type.EMBEDDED).setOfType("Address6819c");

      assertThatThrownBy(() -> database.transaction(() -> {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("@type", "Other6819c");
        map.put("city", "Rome");
        database.newDocument("Person6819c").set("address", map).save();
      })).hasMessageContaining("Address6819c");
    }

    /**
     * The {@code "@type"} source is deliberately not restricted to declared {@code EMBEDDED} properties: it is how a
     * nested typed object survives a JSON round-trip onto a schemaless type, since {@code toMap()}/{@code toJSON()}
     * write the {@code "@type"} back out. Narrowing it would silently downgrade those nested objects to plain maps,
     * so this pins the behaviour rather than leaving it to be "tidied up" later.
     */
    @Test
    void explicitTypeMaterialisesEvenOnAnUndeclaredProperty() {
      database.getSchema().createDocumentType("Address6819e").createProperty("city", Type.STRING);
      database.getSchema().createDocumentType("Person6819e");

      database.transaction(() -> {
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("@type", "Address6819e");
        map.put("city", "Rome");

        database.newDocument("Person6819e").set("address", map).save();
      });

      // Round-trip through the boundary the behaviour exists for: reload, write the record back out with toJSON()
      // (which re-emits the "@type"), and feed that JSON to a fresh document. A regression that dropped the "@type"
      // on the way out, or ignored it on the way back in, would leave a plain map here.
      final Document reloaded = database.query("sql", "select from Person6819e").next().getElement().get();
      assertThat(reloaded.getEmbedded("address").getTypeName()).isEqualTo("Address6819e");
      assertThat(reloaded.getEmbedded("address").getString("city")).isEqualTo("Rome");

      final JSONObject json = reloaded.toJSON(false);
      assertThat(json.getJSONObject("address").getString("@type")).isEqualTo("Address6819e");

      database.transaction(() -> {
        final MutableDocument rebuilt = database.newDocument("Person6819e");
        rebuilt.fromJSON(json);
        rebuilt.save();

        final EmbeddedDocument address = rebuilt.getEmbedded("address");
        assertThat(address).as("the \"@type\" survived the JSON round-trip").isNotNull();
        assertThat(address.getTypeName()).isEqualTo("Address6819e");
        assertThat(address.getString("city")).isEqualTo("Rome");
      });
    }

    /**
     * On a {@code MAP} property the {@code ofType} names the type of the <em>values</em>, not of the map itself, so a
     * map assigned there stays a map: only a property declared {@code EMBEDDED} makes the declaration a statement
     * about the map as a whole.
     */
    @Test
    void aMapPropertyKeepsItsMap() {
      database.getSchema().createDocumentType("Person6819f").createProperty("attributes", Type.MAP).setOfType("STRING");

      database.transaction(() -> {
        final MutableDocument person = database.newDocument("Person6819f").set("attributes", Map.of("city", "Rome"));
        person.save();

        assertThat(person.get("attributes")).isInstanceOf(Map.class);
        assertThat(person.getMap("attributes")).containsEntry("city", "Rome");
      });
    }

    @Test
    void missingTypeAndMissingOfTypeReportsTheMissingKey() {
      database.getSchema().createDocumentType("Person6819d").createProperty("address", Type.EMBEDDED);

      assertThatThrownBy(() -> database.transaction(
          () -> database.newDocument("Person6819d").set("address", Map.of("city", "Rome")).save()))
          .hasMessageContaining("@type")
          .hasMessageContaining("address");
    }
  }

  /**
   * Regression for #5905: storing a numeric value that does not fit an INTEGER/SHORT/BYTE property used to silently truncate
   * it by two's-complement overflow ({@code 3000000000 -> -1294967296}), with no error and no warning. The write must
   * be rejected instead, both through SQL {@code INSERT}/{@code UPDATE} and through the typed record API.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class IntegerTruncation {

    @Test
    void insertOutOfRangeIntegerIsRejectedNotTruncated() throws Exception {
      TestHelper.executeInNewDatabase("issue-5905-insert", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n INTEGER");

        assertThatThrownBy(() -> db.command("sql", "INSERT INTO V SET n = 3000000000, id = 'big'"))
            .isInstanceOf(IllegalArgumentException.class);

        // THE ROW MUST NOT HAVE BEEN WRITTEN WITH A CORRUPTED VALUE
        try (final var rs = db.query("sql", "SELECT FROM V WHERE id = 'big'")) {
          assertThat(rs.hasNext()).isFalse();
        }
      });
    }

    @Test
    void typedApiSetOutOfRangeIntegerIsRejectedNotTruncated() throws Exception {
      TestHelper.executeInNewDatabase("issue-5905-typed-api", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n INTEGER");

        final MutableVertex v = db.newVertex("V");
        assertThatThrownBy(() -> v.set("n", 3_000_000_000L))
            .isInstanceOf(IllegalArgumentException.class);
      });
    }

    @Test
    void updateOutOfRangeShortIsRejected() throws Exception {
      TestHelper.executeInNewDatabase("issue-5905-short", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n SHORT");
        db.command("sql", "INSERT INTO V SET id = 'a'");

        assertThatThrownBy(() -> db.command("sql", "UPDATE V SET n = 40000 WHERE id = 'a'"))
            .isInstanceOf(IllegalArgumentException.class);

        try (final var rs = db.query("sql", "SELECT n FROM V WHERE id = 'a'")) {
          assertThat(rs.next().<Object>getProperty("n")).isNull();
        }
      });
    }

    @Test
    void updateOutOfRangeByteIsRejected() throws Exception {
      TestHelper.executeInNewDatabase("issue-5905-byte", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n BYTE");
        db.command("sql", "INSERT INTO V SET id = 'a'");

        assertThatThrownBy(() -> db.command("sql", "UPDATE V SET n = 200 WHERE id = 'a'"))
            .isInstanceOf(IllegalArgumentException.class);
      });
    }

    @Test
    void inRangeIntegerStillWorks() throws Exception {
      TestHelper.executeInNewDatabase("issue-5905-inrange", db -> {
        db.command("sql", "CREATE VERTEX TYPE V");
        db.command("sql", "CREATE PROPERTY V.n INTEGER");
        db.command("sql", "INSERT INTO V SET n = 2000000000, id = 'ok'");

        try (final var rs = db.query("sql", "SELECT n FROM V WHERE id = 'ok'")) {
          assertThat(((Number) rs.next().getProperty("n")).intValue()).isEqualTo(2_000_000_000);
        }
      });
    }
  }

  /**
   * Regression for #11305: a property declared {@code EMBEDDED}, {@code LINK}, {@code LIST} or {@code MAP OF} a type stores the NAME of that
   * type, and {@code setOfType} refuses a name that resolves to no type. Nothing maintained it afterwards: {@code DROP TYPE} let the
   * type go, and {@code ALTER TYPE ... NAME} renamed it without touching the property, so every write of the property failed with
   * "Type with name 'Address' was not found" and every read of a record holding one dropped the field. Such a property is now a
   * {@link TypeNameDependant}, in the one list the drop guard and the rename carry walk.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PropertyOfTypeDependant extends TestHelper {

    @Override
    protected void beginTest() {
      database.command("sql", "CREATE DOCUMENT TYPE Address");
      database.command("sql", "CREATE PROPERTY Address.city STRING");
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.addr EMBEDDED OF Address");
      database.command("sql", "CREATE PROPERTY Person.others LIST OF Address");
      database.command("sql", "CREATE PROPERTY Person.byName MAP OF Address");
      database.command("sql", "CREATE PROPERTY Person.home LINK OF Address");
      database.command("sql", "CREATE PROPERTY Person.tags LIST OF STRING");
    }

    @Test
    void dropTypeIsRefusedWhileAPropertyIsDeclaredOfIt() {
      assertThatThrownBy(() -> database.command("sql", "DROP TYPE Address")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Person.")
          .hasMessageContaining("DROP PROPERTY Person.");
      assertThat(database.getSchema().existsType("Address")).isTrue();

      // EACH KIND OF DECLARATION HOLDS THE TYPE ON ITS OWN
      for (final String property : List.of("addr", "others", "byName", "home")) {
        assertThatThrownBy(() -> database.getSchema().dropType("Address")).isInstanceOf(SchemaException.class)
            .hasMessageContaining("Person.");
        database.command("sql", "DROP PROPERTY Person." + property);
      }

      // THE LAST DECLARATION GONE, THE TYPE CAN GO
      database.command("sql", "DROP TYPE Address");
      assertThat(database.getSchema().existsType("Address")).isFalse();
    }

    @Test
    void dropTypeByAnAliasIsRefusedToo() {
      database.getSchema().getType("Address").setAliases(Set.of("Addr"));

      assertThatThrownBy(() -> database.getSchema().dropType("Addr")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Person.");
      assertThat(database.getSchema().existsType("Address")).isTrue();
    }

    @Test
    void aPropertyDeclaredByAnAliasHoldsTheTypeAndSurvivesItsRename() {
      database.getSchema().getType("Address").setAliases(Set.of("Addr"));
      database.command("sql", "CREATE PROPERTY Person.work EMBEDDED OF Addr");
      for (final String property : List.of("addr", "others", "byName", "home"))
        database.command("sql", "DROP PROPERTY Person." + property);

      assertThatThrownBy(() -> database.getSchema().dropType("Address")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Person.work");

      // THE ALIAS STAYS WITH THE TYPE ACROSS A RENAME, SO THE NAME THE PROPERTY STORES KEEPS RESOLVING
      database.command("sql", "ALTER TYPE Address NAME Address2");
      assertThat(database.getSchema().getType("Person").getProperty("work").getOfType()).isEqualTo("Addr");
      database.transaction(() -> database.newDocument("Person").set("work", Map.of("city", "Rome")).save());
    }

    @Test
    void aTypeWhoseOwnPropertyIsDeclaredOfItCanBeDropped() {
      database.command("sql", "CREATE DOCUMENT TYPE Node");
      database.command("sql", "CREATE PROPERTY Node.parent LINK OF Node");
      database.command("sql", "CREATE PROPERTY Node.children LIST OF Node");

      database.command("sql", "DROP TYPE Node");
      assertThat(database.getSchema().existsType("Node")).isFalse();
    }

    @Test
    void aScalarListDoesNotHoldAType() {
      for (final String property : List.of("addr", "others", "byName", "home"))
        database.command("sql", "DROP PROPERTY Person." + property);

      database.command("sql", "DROP TYPE Address");
      assertThat(database.getSchema().getType("Person").getProperty("tags").getOfType()).isEqualTo("STRING");
    }

    @Test
    void renameTypeCarriesEveryPropertyDeclaredOfIt() {
      final RID[] target = new RID[1];
      database.transaction(() -> {
        final MutableDocument address = database.newDocument("Address").set("city", "Paris");
        address.save();
        target[0] = address.getIdentity();
      });

      database.command("sql", "ALTER TYPE Address NAME Address2");

      assertCarried();

      // EVERY KIND OF PROPERTY STILL TAKES A WRITE: THEY USED TO THROW "Type with name 'Address' was not found"
      final RID[] written = new RID[1];
      database.transaction(() -> {
        written[0] = database.command("sql", "INSERT INTO Person CONTENT { \"addr\": { \"city\": \"Rome\" }, "
            + "\"others\": [ { \"city\": \"Milan\" } ], \"byName\": { \"y\": { \"city\": \"Naples\" } } }").next().getIdentity().get();
        database.newDocument("Person").set("home", database.lookupByRID(target[0], true)).save();
      });
      final Document person = database.lookupByRID(written[0], true).asDocument();
      assertThat(person.getEmbedded("addr").getTypeName()).isEqualTo("Address2");
      assertThat(person.getEmbedded("addr").getString("city")).isEqualTo("Rome");
      assertThat(person.getList("others")).hasSize(1);
      assertThat(person.getMap("byName")).containsKey("y");

      // THE DROP GUARD FOLLOWS THE NEW NAME
      assertThatThrownBy(() -> database.getSchema().dropType("Address2")).isInstanceOf(SchemaException.class)
          .hasMessageContaining("Person.");

      // AND THE CARRIED NAME IS THE ONE PERSISTED
      reopenDatabase();
      assertCarried();
    }

    /**
     * An embedded document stores its type as the dictionary id of the type's name, which only the creation of a type registered:
     * after a rename no embedded document of the type could be written, declared OF it or not.
     */
    @Test
    void aRenamedTypeTakesNewEmbeddedDocuments() {
      database.command("sql", "CREATE PROPERTY Person.anything EMBEDDED");
      database.command("sql", "ALTER TYPE Address NAME Address2");

      final RID[] written = new RID[1];
      database.transaction(() -> written[0] = database.command("sql",
          "INSERT INTO Person CONTENT { \"anything\": { \"@type\": \"Address2\", \"city\": \"Rome\" } }").next().getIdentity().get());

      reopenDatabase();
      final Document person = database.lookupByRID(written[0], true).asDocument();
      assertThat(person.getEmbedded("anything").getTypeName()).isEqualTo("Address2");
      assertThat(person.getEmbedded("anything").getString("city")).isEqualTo("Rome");
    }

    private void assertCarried() {
      final DocumentType person = database.getSchema().getType("Person");
      for (final String property : List.of("addr", "others", "byName", "home"))
        assertThat(person.getProperty(property).getOfType()).as("Person.%s", property).isEqualTo("Address2");
      assertThat(person.getProperty("tags").getOfType()).isEqualTo("STRING");
    }
  }

  /**
   * Regression for #7589: there was no in-place rename primitive for a property, neither in the Java API
   * nor in SQL - only {@link DocumentType#rename} existed, one level up.
   * <p>
   * The rename implemented here is schema-metadata-only, deliberately: a record stores each field under a small
   * integer id resolved from whatever name it was written with, from a dictionary that is shared database-wide and
   * also referenced by ordinary string VALUES that happen to match an identifier - so there is no cheap way to tell
   * whether renaming that shared id would be safe, and no schema DDL in ArcadeDB today mutates record content as a
   * side effect (not even {@code DROP PROPERTY}, which leaves the field sitting in every document that already had
   * it). A rename that copied values across every record would therefore be a new category of behaviour. Instead,
   * {@code Property.rename}/{@code ALTER PROPERTY ... NAME} only relabels the schema going forward: existing
   * documents keep answering under the old name, and only a write made after the rename lands under the new one -
   * consistent with the same schema/data decoupling every other DDL statement already relies on.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class PropertyRename extends TestHelper {

    private void createType() {
      database.command("sql", "CREATE DOCUMENT TYPE Person");
      database.command("sql", "CREATE PROPERTY Person.name STRING");
      database.command("sql", "ALTER PROPERTY Person.name MANDATORY true");
    }

    @Test
    void javaApiRenamesTheSchemaPropertyAndCarriesMetadataAcross() {
      createType();

      final DocumentType type = database.getSchema().getType("Person");
      final Property renamed = type.getProperty("name").rename("fullName");

      assertThat(type.existsProperty("name")).isFalse();
      assertThat(type.existsProperty("fullName")).isTrue();
      assertThat(renamed.getName()).isEqualTo("fullName");
      assertThat(renamed.getType()).isEqualTo(Type.STRING);
      assertThat(renamed.isMandatory()).as("metadata must carry across the rename").isTrue();
    }

    @Test
    void sqlAlterPropertyNameRenamesTheProperty() {
      createType();

      database.command("sql", "ALTER PROPERTY Person.name NAME fullName");

      final DocumentType type = database.getSchema().getType("Person");
      assertThat(type.existsProperty("name")).isFalse();
      assertThat(type.existsProperty("fullName")).isTrue();
      assertThat(type.getProperty("fullName").isMandatory()).isTrue();
    }

    /**
     * The load-bearing behaviour: a rename does NOT rewrite existing documents. A value already stored under the
     * old name keeps reading back under the old name; a new write after the rename lands under the new name.
     * <p>
     * Which has a consequence worth stating, and which {@code CHECK DATABASE} reports since #7952: renaming a
     * {@code MANDATORY} property leaves every pre-existing document violating that constraint under the NEW name, the
     * same state {@code ALTER PROPERTY ... MANDATORY TRUE} on a populated type produces. Nothing in the engine said so
     * before, and a document in it cannot be updated until the property is supplied - so the check reporting it is
     * the point rather than a nuisance, and this test now pins that it does.
     */
    @Test
    void existingDocumentsAreNotRevisitedByARename() {
      createType();

      database.transaction(() -> database.newDocument("Person").set("name", "Ada").save());

      database.command("sql", "ALTER PROPERTY Person.name NAME fullName");

      database.transaction(() -> database.newDocument("Person").set("fullName", "Grace").save());

      final ResultSet oldRow = database.query("sql", "SELECT FROM Person WHERE name = 'Ada'");
      assertThat(oldRow.hasNext()).as("a pre-existing document keeps answering under the OLD field name").isTrue();
      assertThat(oldRow.next().<Object>getProperty("fullName")).as("it does not retroactively appear under the new name").isNull();

      final ResultSet newRow = database.query("sql", "SELECT FROM Person WHERE fullName = 'Grace'");
      assertThat(newRow.hasNext()).as("a document written after the rename uses the new field name").isTrue();

      // #7952: the pre-existing document now violates the MANDATORY constraint that moved with the name.
      try (final ResultSet check = database.command("sql", "CHECK DATABASE")) {
        assertThat((Long) check.next().getProperty("totalConstraintViolations"))
            .as("the document written before the rename no longer satisfies Person.fullName").isEqualTo(1L);
      }

      // Taken back out so the shared end-of-test integrity assertion, which requires a run with no warnings, still
      // has a clean database to look at.
      database.command("sql", "CHECK DATABASE FIX DELETE INVALID RECORDS").close();
    }

    @Test
    void renamingToAnAlreadyExistingNameIsRefused() {
      createType();
      database.command("sql", "CREATE PROPERTY Person.email STRING");

      assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("email"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already exists");
    }

    @Test
    void renamingANonExistentPropertyIsRefused() {
      createType();

      assertThatThrownBy(() -> database.getSchema().getType("Person").renameProperty("doesNotExist", "newName"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("does not exist");
    }

    @Test
    void renamingToANameAlreadyDefinedInASuperTypeIsRefused() {
      database.command("sql", "CREATE DOCUMENT TYPE Base");
      database.command("sql", "CREATE PROPERTY Base.email STRING");
      createType();
      database.command("sql", "ALTER TYPE Person SUPERTYPE +Base");

      assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("email"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("already defined in a super type");
    }

    @Test
    void aDefaultValueCarriesOverToTheNewNameAndNoLongerAppliesUnderTheOldOne() {
      createType();
      database.command("sql", "ALTER PROPERTY Person.name DEFAULT 'Anonymous'");

      database.command("sql", "ALTER PROPERTY Person.name NAME fullName");

      final DocumentType type = database.getSchema().getType("Person");
      assertThat(type.getProperty("fullName").getDefaultValueDefinition()).isEqualTo("'Anonymous'");
      assertThat(type.getPolymorphicPropertiesWithDefaultDefined()).contains("fullName").doesNotContain("name");
    }

    @Test
    void renamingAnIndexedPropertyIsRefused() {
      createType();
      database.command("sql", "CREATE INDEX ON Person (name) UNIQUE");

      assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("fullName"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("index");

      assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
    }

    /**
     * {@link LocalDocumentType#getAllIndexes(boolean)} only ever walks up (this type and its super types), so an
     * index a SUBTYPE declares on an inherited property is invisible to it. The rename must still refuse: otherwise
     * the subtype's index is left bound to the old name while every write after the rename lands under the new one.
     */
    @Test
    void renamingAPropertyIndexedOnlyByASubtypeIsRefused() {
      createType();
      database.command("sql", "CREATE DOCUMENT TYPE Employee");
      database.command("sql", "ALTER TYPE Employee SUPERTYPE +Person");
      database.command("sql", "CREATE INDEX ON Employee (name) NOTUNIQUE");

      assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("fullName"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("index");

      assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
    }

    /**
     * Same shape as {@link #renamingAPropertyIndexedOnlyByASubtypeIsRefused}, one level deeper: the index sits on a
     * subtype of a subtype, exercising {@code findDescendantIndexOnProperty}'s recursive case rather than just its
     * first level.
     */
    @Test
    void renamingAPropertyIndexedOnlyByAGrandchildSubtypeIsRefused() {
      createType();
      database.command("sql", "CREATE DOCUMENT TYPE Employee");
      database.command("sql", "ALTER TYPE Employee SUPERTYPE +Person");
      database.command("sql", "CREATE DOCUMENT TYPE Manager");
      database.command("sql", "ALTER TYPE Manager SUPERTYPE +Employee");
      database.command("sql", "CREATE INDEX ON Manager (name) NOTUNIQUE");

      assertThatThrownBy(() -> database.getSchema().getType("Person").getProperty("name").rename("fullName"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("index");

      assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
    }

    /**
     * A subtype may independently declare its own property under a name a super type also uses -
     * {@code addSuperType}'s own conflict check only warns about this, never refuses it - and from that subtype
     * down, the name resolves to the subtype's shadowing property, not the super type's. An index on the shadowing
     * property must not block renaming the super type's unrelated, same-named property.
     */
    @Test
    void renamingAPropertyIsNotBlockedByAnIndexOnAnUnrelatedShadowingSubtypeProperty() {
      database.command("sql", "CREATE DOCUMENT TYPE Employee");
      database.command("sql", "CREATE PROPERTY Employee.name STRING");
      database.command("sql", "CREATE INDEX ON Employee (name) NOTUNIQUE");

      createType();
      // Links Employee under Person with both already independently declaring "name" - a conflict addSuperType only
      // warns about, producing the shadow this test targets.
      database.command("sql", "ALTER TYPE Employee SUPERTYPE +Person");

      final Property renamed = database.getSchema().getType("Person").getProperty("name").rename("fullName");

      assertThat(renamed.getName()).isEqualTo("fullName");
      assertThat(database.getSchema().getType("Employee").existsProperty("name"))
          .as("Employee's own shadowing property is unrelated to Person's rename").isTrue();
    }

    @Test
    void sqlAlterPropertyNameAcceptsABacktickQuotedNameWithSpaces() {
      createType();

      database.command("sql", "ALTER PROPERTY Person.name NAME `full name`");

      final DocumentType type = database.getSchema().getType("Person");
      assertThat(type.existsProperty("name")).isFalse();
      assertThat(type.existsProperty("full name")).isTrue();
      assertThat(type.getProperty("full name").isMandatory()).isTrue();
    }

    @Test
    void renamingADeclaredTimeSeriesColumnIsRefused() {
      database.command("sql", "CREATE TIMESERIES TYPE Reading TIMESTAMP ts TAGS (sensor STRING) FIELDS (value DOUBLE)");

      assertThatThrownBy(() -> database.getSchema().getType("Reading").getProperty("value").rename("v"))
          .isInstanceOf(SchemaException.class)
          .hasMessageContaining("TIMESERIES");
    }

    @Test
    void aStaleHandleFromBeforeTheRenameCannotBeUsedAgain() {
      createType();

      final Property original = database.getSchema().getType("Person").getProperty("name");
      original.rename("fullName");

      assertThatThrownBy(() -> original.rename("somethingElse")).isInstanceOf(SchemaException.class);
    }

    @Test
    void renamingToItsOwnCurrentNameIsANoOp() {
      createType();

      final Property property = database.getSchema().getType("Person").getProperty("name");
      final Property result = property.rename("name");

      assertThat(result.getName()).isEqualTo("name");
      assertThat(database.getSchema().getType("Person").existsProperty("name")).isTrue();
    }
  }
}
