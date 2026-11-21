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
package com.arcadedb.remote;

import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.BaseGraphServerTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Values keep their type through the HTTP {@link RemoteDatabase}: dates (inherited DATE properties, date parameters,
 * BC dates in a custom format), {@code count(*)} as a Long, {@code MAP/LIST OF LONG} on read and on save, the type a mutable
 * record answers, and whole vertices returned by MATCH or SELECT.
 * <p>
 * Each nested class keeps its own server fixture (database name, hooks), so they cannot share one.
 */
class RemoteTypeFidelityIT {

  /**
   * Issue #10865: the remote Java client sent a date as epoch MILLISECONDS, which the server reads as epoch DAYS for a DATE, when
   * the DATE property was declared on a supertype of the record's type, and whenever the date was bound as a query parameter.
   * 26.10.2 refused the write with "Invalid value for EpochDay"; earlier versions stored null.
   */
  @Nested
  class InheritedDateAndDateParameter extends BaseGraphServerTest {
    private static final String    DATABASE_NAME = "remote-database-10865";
    private static final LocalDate DAY           = LocalDate.of(2024, 1, 15);

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Test
    void dateDeclaredOnTheTypeItself() {
      final RemoteDatabase db = schema();
      db.transaction(() -> db.newDocument("Evento").set("fecha", DAY).save());
      assertThat(dates(db, "Evento")).containsExactly(DAY);
    }

    @Test
    void localDateForADateInheritedFromASupertype() {
      final RemoteDatabase db = schema();
      db.transaction(() -> {
        db.newVertex("Hijo").set("fecha", DAY).save();
        db.newVertex("Nieto").set("fecha", DAY).save();
      });
      // SELECT FROM Hijo is polymorphic: it reads the Nieto record too
      assertThat(dates(db, "Hijo")).containsExactly(DAY, DAY);
      assertThat(dates(db, "Nieto")).containsExactly(DAY);
    }

    @Test
    void javaUtilDateForADateInheritedFromASupertype() {
      final RemoteDatabase db = schema();
      final Date afternoon = Date.from(DAY.atTime(15, 30).toInstant(ZoneOffset.UTC));
      db.transaction(() -> {
        db.newVertex("Hijo").set("fecha", afternoon).save();
        db.newVertex("Nieto").set("fecha", afternoon).save();
      });
      // SELECT FROM Hijo is polymorphic: it reads the Nieto record too
      assertThat(dates(db, "Hijo")).containsExactly(DAY, DAY);
      assertThat(dates(db, "Nieto")).containsExactly(DAY);
    }

    @Test
    void updateOfAnInheritedDate() {
      final RemoteDatabase db = schema();
      db.transaction(() -> db.newVertex("Nieto").set("fecha", DAY).save());
      final Date nextDay = Date.from(DAY.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC));
      db.transaction(() -> {
        try (final ResultSet rs = db.query("sql", "SELECT FROM Nieto")) {
          rs.next().toElement().asVertex().modify().set("fecha", nextDay).save();
        }
      });
      assertThat(dates(db, "Nieto")).containsExactly(DAY.plusDays(1));
    }

    @Test
    void schemaSeesAPropertyDeclaredTwoLevelsUp() {
      final RemoteDatabase db = schema();
      assertThat(db.getSchema().getType("Hijo").existsPolymorphicProperty("fecha")).isTrue();
      assertThat(db.getSchema().getType("Nieto").existsPolymorphicProperty("fecha")).isTrue();
      assertThat(db.getSchema().getType("Nieto").existsPolymorphicProperty("missing")).isFalse();
    }

    @Test
    void localDateBoundAsAPositionalParameter() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fecha = ?", DAY);
      db.command("sql", "INSERT INTO Hijo SET fecha = ?", DAY);
      assertThat(dates(db, "Evento")).containsExactly(DAY);
      assertThat(dates(db, "Hijo")).containsExactly(DAY);
    }

    @Test
    void localDateBoundAsANamedParameter() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fecha = :fecha", Map.of("fecha", DAY));
      assertThat(dates(db, "Evento")).containsExactly(DAY);
    }

    @Test
    void localDatesInAListParameter() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fechas = ?", List.of(DAY, DAY.plusDays(1)));
      try (final ResultSet rs = db.query("sql", "SELECT fechas FROM Evento")) {
        final List<Object> read = rs.next().getProperty("fechas");
        // Issue #10909: the items come back as LocalDate, like a scalar DATE, not as their text
        assertThat(read).containsExactly(DAY, DAY.plusDays(1));
      }
    }

    @Test
    void localDatesInAListOfTheDocumentRead() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fechas = ?", List.of(DAY, DAY.plusDays(1)));
      try (final ResultSet rs = db.query("sql", "SELECT FROM Evento")) {
        assertThat(rs.next().<List<Object>>getProperty("fechas")).containsExactly(DAY, DAY.plusDays(1));
      }
    }

    @Test
    void anEmptyListOfDatesStillReads() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fechas = []");
      try (final ResultSet rs = db.query("sql", "SELECT FROM Evento")) {
        assertThat(rs.next().<List<Object>>getProperty("fechas")).isEmpty();
      }
    }

    @Test
    void localDatesInAnUntypedProjectedList() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fechas = ?", List.of(DAY, DAY.plusDays(1)));
      try (final ResultSet rs = db.query("sql", "SELECT fechas.asList() AS d, [fecha] AS single FROM Evento")) {
        final Object read = rs.next().getProperty("d");
        assertThat(read).isEqualTo(List.of(DAY, DAY.plusDays(1)));
      }
    }

    /**
     * Issue #10908: a comparison reads a number as epoch days, so the epoch milliseconds the client used to send for a LocalDate never
     * matched. The date goes as a typed marker now.
     */
    @Test
    void localDateInAComparison() {
      final RemoteDatabase db = schema();
      db.command("sql", "INSERT INTO Evento SET fecha = ?", DAY);
      db.command("sql", "INSERT INTO Evento SET fecha = ?", DAY.plusDays(1));
      try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha = ?", DAY)) {
        assertThat(rs.next().<LocalDate>getProperty("fecha")).isEqualTo(DAY);
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha = :d", Map.of("d", DAY))) {
        assertThat(rs.next().<LocalDate>getProperty("fecha")).isEqualTo(DAY);
        assertThat(rs.hasNext()).isFalse();
      }
      try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha IN ?", List.of(DAY, DAY.plusDays(1)))) {
        int count = 0;
        while (rs.hasNext()) {
          rs.next();
          count++;
        }
        assertThat(count).isEqualTo(2);
      }
      try (final ResultSet rs = db.query("sql", "SELECT FROM Evento WHERE fecha > ?", DAY)) {
        assertThat(rs.next().<LocalDate>getProperty("fecha")).isEqualTo(DAY.plusDays(1));
        assertThat(rs.hasNext()).isFalse();
      }
    }

    @Test
    void midnightMillisInARecordContent() {
      final RemoteDatabase db = schema();
      final long millis = DAY.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      db.command("sql", "INSERT INTO Evento CONTENT {\"fecha\": " + millis + "}");
      db.command("sql", "INSERT INTO Evento CONTENT {\"fecha\": " + DAY.toEpochDay() + "}");
      assertThat(dates(db, "Evento")).containsExactly(DAY, DAY);
    }

    private RemoteDatabase schema() {
      final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      db.command("sqlscript", """
          CREATE DOCUMENT TYPE Evento;
          CREATE PROPERTY Evento.fecha DATE;
          CREATE PROPERTY Evento.fechas LIST OF DATE;
          CREATE VERTEX TYPE Base;
          CREATE PROPERTY Base.fecha DATE;
          CREATE VERTEX TYPE Hijo EXTENDS Base;
          CREATE VERTEX TYPE Nieto EXTENDS Hijo;
          """);
      return db;
    }

    private static List<Object> dates(final RemoteDatabase db, final String typeName) {
      final List<Object> result = new ArrayList<>();
      try (final ResultSet rs = db.query("sql", "SELECT fecha FROM " + typeName)) {
        while (rs.hasNext())
          result.add(rs.next().getProperty("fecha"));
      }
      return result;
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }
  }

  // Regression: #1515 a DATE before Christ written and read in a custom format with the era ('dd MM yyyy GG') must read
  // back in that format over the remote client.
  @Nested
  class DatesBeforeChrist extends BaseGraphServerTest {

    protected String getDatabaseName() {
      return "issue1515";
    }

    @Test
    void bcAndAdDatesReadBackInTheDatabaseDateFormat() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      String script = """
          alter database `arcadedb.dateImplementation` `java.time.LocalDate`;
          alter database `arcadedb.dateTimeImplementation` `java.time.LocalDateTime`;
          alter database `arcadedb.dateFormat` 'dd MM yyyy GG';
          alter database `arcadedb.dateTimeFormat` 'dd MM yyyy GG HH:mm:ss';
          create property Person.name if not exists String (mandatory true, notnull true);
          create index if not exists on Person (name) unique;
          create property Person.dateOfBirth if not exists Date;
          create property Person.dateOfDeath if not exists Date;
          """;

      database.transaction(() ->
          database.command("sqlscript", script));

      database.transaction(() ->
          database.command("sql", """
              insert into Person set name = 'Hannibal',
              dateOfBirth = date('01 01 0001 BC', 'dd MM yyyy GG'),
              dateOfDeath = date('01 01 0001 AD', 'dd MM yyyy GG')
              """));

      ResultSet result = database.query("sql", "select from Person where name = 'Hannibal'");
      Result doc = result.next();
      assertThat(doc.<String>getProperty("dateOfBirth")).isEqualTo("01 01 0001 BC");
      assertThat(doc.<String>getProperty("dateOfDeath")).isEqualTo("01 01 0001 AD");

    }
  }

  /**
   * Regression test for issue #4267: the type returned by {@code SELECT count(*)} over the HTTP
   * RemoteDatabase must match what the gRPC client returns (both should be {@code Long}, matching
   * the engine's {@link com.arcadedb.function.sql.math.SQLFunctionCount} which produces a long).
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class CountType extends BaseGraphServerTest {

    @Override
    protected String getDatabaseName() {
      return "issue4267";
    }

    @Test
    void countStarReturnsLongOverHttp() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "create vertex type SimpleVertexEx");

      final String rid;
      database.begin();
      try {
        final MutableVertex svt1 = database.newVertex("SimpleVertexEx");
        svt1.set("svex", "svt1");
        svt1.save();
        database.commit();

        rid = svt1.getIdentity().toString();
        database.begin();
        svt1.set("svex", rid);
        svt1.save();
      } finally {
        database.commit();
      }

      final ResultSet rs = database.query("sql", "select count(*) from SimpleVertexEx where svex = ?", rid);
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      final String column = row.getPropertyNames().iterator().next();
      final Object value = row.getProperty(column);
      assertThat(value).isInstanceOf(Long.class);
      assertThat(((Number) value).longValue()).isEqualTo(1L);
    }

    @Test
    void aliasedCountReturnsLongOverHttp() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "create document type Doc4267");
      for (int i = 0; i < 3; i++) {
        final MutableDocument d = database.newDocument("Doc4267");
        d.set("n", i);
        d.save();
      }

      final ResultSet rs = database.query("sql", "select count(*) as c from Doc4267");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("c")).isInstanceOf(Long.class);
      assertThat(row.<Number>getProperty("c").longValue()).isEqualTo(3L);
    }
  }

  /**
   * Reproduces issue #4735: a property declared as {@code MAP LONG} (or {@code LIST LONG}) returns the nested values as
   * {@link Integer} instead of {@link Long} through the remote Java client when the numeric value fits in the integer range.
   */
  @Nested
  class MapLongRead extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-database-4735";

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Test
    void mapLongValuesAreHydratedAsLong() throws Exception {
      testEachServer(serverIndex -> {
        final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(serverIndex), DATABASE_NAME, "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

        db.command("sqlscript", """
            create vertex type MapLongTest if not exists;
            create property MapLongTest.values if not exists MAP OF LONG;
            create property MapLongTest.list if not exists LIST OF LONG;
            insert into MapLongTest set values = {'first': 1001L, 'second': 2002L}, list = [10L, 20L, 30L];
            """);

        try (final ResultSet rs = db.query("sql", "select from MapLongTest")) {
          assertThat(rs.hasNext()).isTrue();
          final Result row = rs.next();

          final Map<String, Object> values = row.getProperty("values");
          assertThat(values.get("first")).isInstanceOf(Long.class);
          assertThat(values.get("second")).isInstanceOf(Long.class);
          assertThat(values.get("first")).isEqualTo(1001L);
          assertThat(values.get("second")).isEqualTo(2002L);

          final List<Object> list = row.getProperty("list");
          for (final Object item : list)
            assertThat(item).isInstanceOf(Long.class);
          assertThat(list).containsExactly(10L, 20L, 30L);
        }
      });
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }
  }

  /**
   * Reproduces issue #5261 (write-side follow-up to #4735): saving a remote record with a {@code MAP OF LONG} / {@code LIST OF LONG}
   * property must not fail schema validation. The remote client re-serializes the full record with {@code UPDATE ... CONTENT}, and
   * small {@code Long} values arrive as untyped JSON integers parsed as {@code Integer}; the server must coerce them back to the
   * declared nested {@code LONG} type before validation.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MapLongSave extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-database-5261";

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Test
    void remoteSavePreservesNestedLongTypes() throws Exception {
      testEachServer(serverIndex -> {
        final RemoteDatabase db = new RemoteDatabase("127.0.0.1", getServerHttpPort(serverIndex), DATABASE_NAME, "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

        db.command("sqlscript", """
            create vertex type MapLongTest if not exists;
            create property MapLongTest.values if not exists MAP OF LONG;
            create property MapLongTest.numbers if not exists LIST OF LONG;
            create property MapLongTest.note if not exists STRING;
            insert into MapLongTest set values = {'first': 1001L, 'second': 2002L}, numbers = [10L, 20L, 30L], note = 'before';
            """);

        final RID rid;
        try (final ResultSet result = db.query("sql", "select from MapLongTest")) {
          assertThat(result.hasNext()).isTrue();
          final MutableVertex vertex = result.next().toElement().asVertex().modify();

          final Map<String, Object> values = (Map<String, Object>) vertex.get("values");
          assertThat(values.get("first")).isInstanceOf(Long.class);

          // Only an unrelated property is changed: RemoteDatabase.saveRecord() sends the complete record with UPDATE ... CONTENT,
          // including values/numbers, which must not be rejected by validation.
          vertex.set("note", "after");
          vertex.save();
          rid = vertex.getIdentity();
        }

        // Reload and verify the nested values are still LONG (as fixed by #4735).
        try (final ResultSet result = db.query("sql", "select from " + rid)) {
          final Result row = result.next();

          final Map<String, Object> values = row.getProperty("values");
          assertThat(values.get("first")).isInstanceOf(Long.class).isEqualTo(1001L);
          assertThat(values.get("second")).isInstanceOf(Long.class).isEqualTo(2002L);

          final List<Object> numbers = row.getProperty("numbers");
          for (final Object item : numbers)
            assertThat(item).isInstanceOf(Long.class);
          assertThat(numbers).containsExactly(10L, 20L, 30L);

          assertThat(row.<String>getProperty("note")).isEqualTo("after");
        }
      });
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
    }

    @AfterEach
    public void endTest() {
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }
  }

  /**
   * Issue #11053: {@code getType()} threw {@code UnsupportedOperationException} on every remote mutable record (document, vertex
   * and edge), although the record holds its type and the immutable record it was copied from answers it, so generic code
   * written against {@link Document} broke the moment a record became mutable. The mutable and immutable records are a pair and
   * are checked as one.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class MutableRecordType extends BaseGraphServerTest {
    private static final String DATABASE_NAME = "remote-database-11053";

    private RemoteDatabase remote;

    @Override
    protected boolean isCreateDatabases() {
      return false;
    }

    @Test
    void aNewRecordAnswersItsType() {
      final MutableDocument document = remote.newDocument("Doc");
      assertThat(document.getType()).isInstanceOf(DocumentType.class);
      assertThat(document.getType().getName()).isEqualTo("Doc");

      final MutableVertex vertex = remote.newVertex("Person");
      assertThat(vertex.getType()).isInstanceOf(VertexType.class);
      assertThat(vertex.getType().getName()).isEqualTo("Person");
    }

    @Test
    void aModifiedRecordAnswersWhatTheRecordItWasCopiedFromAnswers() {
      remote.transaction(() -> {
        remote.newDocument("Doc").set("name", "doc", "n", 1).save();
        final MutableVertex a = remote.newVertex("Person").set("name", "a").save();
        final MutableVertex b = remote.newVertex("Person").set("name", "b").save();
        final MutableEdge edge = a.newEdge("Knows", b, "since", 2020);
        assertThat(edge.getType()).isInstanceOf(EdgeType.class);
        assertThat(edge.getType().getName()).isEqualTo("Knows");
      });

      assertPair(readOne("SELECT FROM Doc"), DocumentType.class);
      assertPair(readOne("SELECT FROM Person WHERE name = 'a'"), VertexType.class);
      assertPair(readOne("SELECT FROM Knows"), EdgeType.class);
    }

    /** The type a modified record answers is the one its save writes to. */
    @Test
    void aModifiedRecordSavesThroughItsType() {
      remote.transaction(() -> remote.newVertex("Person").set("name", "a").save());
      remote.transaction(() -> {
        final MutableVertex modified = ((Vertex) readOne("SELECT FROM Person")).modify();
        assertThat(modified.getType().getName()).isEqualTo("Person");
        modified.set("name", "renamed").save();
      });
      assertThat(readOne("SELECT FROM Person").getString("name")).isEqualTo("renamed");
    }

    private Document readOne(final String query) {
      try (final ResultSet rs = remote.query("sql", query)) {
        return rs.next().toElement();
      }
    }

    private static void assertPair(final Document read, final Class<? extends DocumentType> kind) {
      final MutableDocument modified = read.modify();

      assertThat(read.getType()).isInstanceOf(kind);
      assertThat(modified.getType()).isSameAs(read.getType());
      assertThat(modified.getTypeName()).isEqualTo(read.getTypeName());
      assertThat(modified.getIdentity()).isEqualTo(read.getIdentity());
      assertThat(modified.getPropertyNames()).isEqualTo(read.getPropertyNames());
      for (final String property : read.getPropertyNames()) {
        assertThat(modified.has(property)).as(property).isEqualTo(read.has(property));
        assertThat(modified.get(property)).as(property).isEqualTo(read.get(property));
      }
      assertThat(modified.toMap()).isEqualTo(read.toMap());
      assertThat(modified.toJSON().toMap()).isEqualTo(read.toJSON().toMap());
    }

    @BeforeEach
    public void beginTest() {
      super.beginTest();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      if (!server.exists(DATABASE_NAME))
        server.create(DATABASE_NAME);
      remote = new RemoteDatabase("127.0.0.1", getServerHttpPort(), DATABASE_NAME, "root", DEFAULT_PASSWORD_FOR_TESTS);
      remote.command("sqlscript", """
          CREATE DOCUMENT TYPE Doc;
          CREATE VERTEX TYPE Person;
          CREATE EDGE TYPE Knows;
          """);
    }

    @AfterEach
    public void endTest() {
      if (remote != null)
        remote.close();
      final RemoteServer server = new RemoteServer("127.0.0.1", getServerHttpPort(), "root", DEFAULT_PASSWORD_FOR_TESTS);
      if (server.exists(DATABASE_NAME))
        server.drop(DATABASE_NAME);
      super.endTest();
    }
  }

  /**
   * Regression test for issue #4689 via the RemoteDatabase client (HTTP "record" serializer path,
   * parsed through json2Result/json2Record) - the path a remote driver actually uses.
   */
  @Nested
  class WholeVertexReturned extends BaseGraphServerTest {

    @Override
    protected String getDatabaseName() {
      return "issue4689";
    }

    @Test
    void cypherMatchReturnWholeVertexOverRemote() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("opencypher", "CREATE (u:IssueUser {name: 'Alice', age: 30})");
      database.command("opencypher", "CREATE (u:IssueUser {name: 'Bob', age: 25})");

      // The reported failing query: returning the whole vertex
      final ResultSet rs = database.query("opencypher", "MATCH (u:IssueUser) RETURN u");

      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.isVertex()).as("RETURN u should produce a vertex element over RemoteDatabase").isTrue();
        final Vertex v = row.getVertex().get();
        assertThat(v.getString("name")).isNotNull();
        count++;
      }
      assertThat(count).as("Should return 2 User vertices over RemoteDatabase").isEqualTo(2);
    }

    @Test
    void cypherMatchReturnVertexProjectionOverRemote() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("opencypher", "CREATE (u:IssueUserProj {name: 'Charlie', age: 35})");

      // The workaround the reporter says works
      final ResultSet rs = database.query("opencypher", "MATCH (u:IssueUserProj) RETURN u{.*} as user");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<Object>getProperty("user")).as("Projection should expose the 'user' property").isNotNull();
    }

    @Test
    void sqlSelectWholeRecordOverRemote() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "CREATE VERTEX TYPE SqlRemoteAll");
      database.command("sql", "INSERT INTO SqlRemoteAll SET name = 'Alice', age = 30");
      database.command("sql", "INSERT INTO SqlRemoteAll SET name = 'Bob', age = 25");

      // SQL: returning the whole record (same class of issue as Cypher)
      final ResultSet rs = database.query("sql", "SELECT FROM SqlRemoteAll");

      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.isVertex()).as("SELECT FROM should produce a vertex element over RemoteDatabase").isTrue();
        count++;
      }
      assertThat(count).as("Should return 2 records over RemoteDatabase").isEqualTo(2);
    }

    @Test
    void sqlSelectFieldOverRemote() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("sql", "CREATE VERTEX TYPE SqlRemoteFields");
      database.command("sql", "INSERT INTO SqlRemoteFields SET name = 'Dave', age = 40");

      // SQL workaround: selecting an individual field
      final ResultSet rs = database.query("sql", "SELECT name FROM SqlRemoteFields");

      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      assertThat(row.<String>getProperty("name")).isEqualTo("Dave");
    }

    @Test
    void cypherMatchReturnVertexWithEdgesOverRemote() {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", getServerHttpPort(), getDatabaseName(), "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      database.command("opencypher", "CREATE (u:UserEdgeRemote {name: 'Src', role: 'admin'})");
      database.command("opencypher", "CREATE (u:UserEdgeRemote {name: 'Dst', role: 'user'})");
      database.command("sql",
          "CREATE EDGE E1 FROM (SELECT FROM UserEdgeRemote WHERE name = 'Src') TO (SELECT FROM UserEdgeRemote WHERE name = 'Dst')");

      // Vertices with edges - exercises the vertex serialization with edge metadata over the wire
      final ResultSet rs = database.query("opencypher", "MATCH (u:UserEdgeRemote) RETURN u");

      int count = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.isVertex()).isTrue();
        count++;
      }
      assertThat(count).as("Should return 2 vertices with edges over RemoteDatabase").isEqualTo(2);
    }
  }
}
