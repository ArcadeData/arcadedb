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

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.AlterTypeStatement;
import com.arcadedb.query.sql.parser.CreateDocumentTypeStatement;
import com.arcadedb.query.sql.parser.CreateEdgeTypeStatement;
import com.arcadedb.query.sql.parser.CreateIndexStatement;
import com.arcadedb.query.sql.parser.CreatePropertyStatement;
import com.arcadedb.query.sql.parser.CreateVertexTypeStatement;
import com.arcadedb.query.sql.parser.DropTypeStatement;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for issue #7914: the remote API built its SQL by concatenating a name between two back-ticks
 * with NO escaping, so a name containing a backslash reached the server as a DIFFERENT name and a name ending in
 * one was a parse error.
 * <p>
 * {@code RemoteSchema.createDocumentType("a\b")} used to send {@code create document type `a\b`}; the server's
 * lexer consumes the backslash as an escape introducer, so the type was created as {@code ab} and the client's own
 * follow-up {@code getType("a\b")} then failed over a type that had been fully created under a name the caller
 * never asked for. That is verbatim the consequence #7740 item 1 described for the time-series builder, whose fix
 * extracted {@link com.arcadedb.query.sql.parser.Identifier#quote} - and left the rest of the remote package
 * hand-rolling it.
 * <p>
 * These assertions are not "the string looks escaped": each one PARSES what the client would have sent with the
 * engine's own SQL parser and asserts the name the server would resolve is the one the caller passed. A test that
 * only compared against a hand-written expected string would agree with any escaping scheme, including a wrong one.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7914RemoteIdentifierQuotingTest {

  /**
   * The three shapes that reach the lexer: a backslash mid-name (silently renames), a TRAILING backslash (swallows
   * the closing back-tick, so the statement no longer parses at all) and a back-tick (closes the quote early).
   */
  private static final List<String> HOSTILE_NAMES = List.of("a\\b", "trailing\\", "back`tick", "both\\`mixed");

  private RemoteDatabase remoteDatabase;

  @BeforeEach
  void setUp() {
    remoteDatabase = mock(RemoteDatabase.class);
    final ResultSet empty = mock(ResultSet.class);
    when(empty.hasNext()).thenReturn(false);
    when(remoteDatabase.command(anyString(), anyString())).thenReturn(empty);
    when(remoteDatabase.command(anyString(), anyString(), any(Map.class))).thenReturn(empty);
    when(remoteDatabase.query(anyString(), anyString())).thenReturn(empty);
    when(remoteDatabase.query(anyString(), anyString(), any(Map.class))).thenReturn(empty);
  }

  @ParameterizedTest
  @ValueSource(strings = { "a\\b", "trailing\\", "back`tick", "both\\`mixed" })
  void createDocumentTypeNamesTheTypeTheCallerAskedFor(final String typeName) {
    final RemoteSchema schema = new RemoteSchema(remoteDatabase);
    try {
      schema.createDocumentType(typeName);
    } catch (final RuntimeException ignore) {
      // the mocked result set is empty, so the client reports the creation as failed - what is under test is the
      // STATEMENT it sent before that, captured below.
    }

    final CreateDocumentTypeStatement statement = (CreateDocumentTypeStatement) parse(commandSent());
    assertThat(statement.name.getStringValue()).isEqualTo(typeName);
  }

  @ParameterizedTest
  @ValueSource(strings = { "a\\b", "trailing\\", "back`tick", "both\\`mixed" })
  void createVertexTypeNamesTheTypeTheCallerAskedFor(final String typeName) {
    final RemoteSchema schema = new RemoteSchema(remoteDatabase);
    try {
      schema.createVertexType(typeName);
    } catch (final RuntimeException ignore) {
    }

    final CreateVertexTypeStatement statement = (CreateVertexTypeStatement) parse(commandSent());
    assertThat(statement.name.getStringValue()).isEqualTo(typeName);
  }

  @ParameterizedTest
  @ValueSource(strings = { "a\\b", "trailing\\", "back`tick", "both\\`mixed" })
  void createEdgeTypeNamesTheTypeTheCallerAskedFor(final String typeName) {
    final RemoteSchema schema = new RemoteSchema(remoteDatabase);
    try {
      schema.createEdgeType(typeName);
    } catch (final RuntimeException ignore) {
    }

    final CreateEdgeTypeStatement statement = (CreateEdgeTypeStatement) parse(commandSent());
    assertThat(statement.name.getStringValue()).isEqualTo(typeName);
  }

  @Test
  void dropTypeNamesTheTypeTheCallerAskedFor() {
    for (final String typeName : HOSTILE_NAMES) {
      final RemoteDatabase db = mock(RemoteDatabase.class);
      new RemoteSchema(db).dropType(typeName);

      final DropTypeStatement statement = (DropTypeStatement) parse(commandSent(db));
      assertThat(statement.name.getStringValue()).isEqualTo(typeName);
    }
  }

  /**
   * Both halves of a qualified property name, on the one line that used to spell the escaping two ways at once:
   * {@code alter property `type`.`prop` name Identifier.quote(newName)}.
   */
  @Test
  void createPropertyNamesBothTheTypeAndThePropertyTheCallerAskedFor() {
    for (final String hostile : HOSTILE_NAMES) {
      final RemoteDatabase db = mock(RemoteDatabase.class);
      final RemoteDocumentType type = documentType(db, hostile);
      try {
        type.createProperty(hostile + "prop", "STRING");
      } catch (final RuntimeException ignore) {
      }

      final CreatePropertyStatement statement = (CreatePropertyStatement) parse(commandSent(db));
      assertThat(statement.typeName.getStringValue()).isEqualTo(hostile);
      assertThat(statement.propertyName.getStringValue()).isEqualTo(hostile + "prop");
    }
  }

  @Test
  void addSuperTypeNamesBothTypesTheCallerAskedFor() {
    for (final String hostile : HOSTILE_NAMES) {
      final RemoteDatabase db = mock(RemoteDatabase.class);
      final RemoteDocumentType type = documentType(db, hostile);
      try {
        type.addSuperType(hostile + "super");
      } catch (final RuntimeException ignore) {
        // the follow-up schema reload has nothing to reload from in a mock; the statement is already captured
      }

      final AlterTypeStatement statement = (AlterTypeStatement) parse(commandSent(db));
      assertThat(statement.name.getStringValue()).isEqualTo(hostile);
      assertThat(statement.items).hasSize(1);
      assertThat(statement.items.getFirst().identifierListValue.getFirst().getStringValue()).isEqualTo(hostile + "super");
    }
  }

  /**
   * An index over properties whose names need escaping: the property list used to be joined RAW, with not even a
   * back-tick around each name, so a name carrying the separator was parsed as SEVERAL properties.
   */
  @Test
  void createTypeIndexNamesTheTypeAndPropertiesTheCallerAskedFor() {
    final RemoteDatabase db = mock(RemoteDatabase.class);
    new RemoteSchema(db).createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "a\\b", "p,q", "back`tick");

    final CreateIndexStatement statement = (CreateIndexStatement) parse(commandSent(db));
    assertThat(statement.typeName.getStringValue()).isEqualTo("a\\b");
    assertThat(statement.propertyList).hasSize(2);
    assertThat(statement.propertyList.get(0).name.getStringValue()).isEqualTo("p,q");
    assertThat(statement.propertyList.get(1).name.getStringValue()).isEqualTo("back`tick");
  }

  /**
   * {@code existsTrigger} interpolated the name into a STRING LITERAL instead of binding it, which is the same
   * defect with the other quote character: every sibling {@code exists*()} in the class already binds a parameter.
   */
  @Test
  void existsTriggerBindsTheNameRatherThanInterpolatingIt() {
    final RemoteDatabase db = mock(RemoteDatabase.class);
    final ResultSet empty = mock(ResultSet.class);
    when(db.command(anyString(), anyString(), any(Map.class))).thenReturn(empty);

    new RemoteSchema(db).existsTrigger("it's");

    final ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map<String, Object>> params = ArgumentCaptor.forClass(Map.class);
    verify(db).command(anyString(), sql.capture(), params.capture());

    assertThat(sql.getValue()).doesNotContain("it's");
    assertThat(params.getValue()).containsEntry("name", "it's");
  }

  /**
   * {@code isConnectedTo} interpolated the edge type into a STRING LITERAL argument of {@code both()}/{@code out()},
   * so a type name carrying a quote ended the literal early and had its remainder parsed as more SQL. The name is
   * bound now: {@code both()} takes its labels from ALREADY-EVALUATED argument values, so a parameter is exactly
   * as good as a literal there and cannot be mis-parsed.
   */
  @Test
  void isConnectedToBindsTheEdgeTypeRatherThanInterpolatingIt() {
    final RemoteDatabase db = mock(RemoteDatabase.class);
    final ResultSet empty = mock(ResultSet.class);
    when(db.query(anyString(), anyString(), any(Map.class))).thenReturn(empty);

    final Vertex from = mock(Vertex.class);
    when(from.getIdentity()).thenReturn(new RID(1, 2));

    new RemoteVertex(from, db).isConnectedTo(new RID(3, 4), Vertex.DIRECTION.BOTH, "it's`hostile");

    final ArgumentCaptor<String> sql = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map<String, Object>> params = ArgumentCaptor.forClass(Map.class);
    verify(db).query(anyString(), sql.capture(), params.capture());

    assertThat(sql.getValue()).doesNotContain("hostile");
    assertThat(params.getValue()).containsEntry("edgeType", "it's`hostile");
  }

  private RemoteDocumentType documentType(final RemoteDatabase db, final String typeName) {
    final RemoteSchema schema = new RemoteSchema(db);
    when(db.getSchema()).thenReturn(schema);
    return new RemoteDocumentType(db, new ResultInternal(Map.of("name", typeName, "properties", List.of())));
  }

  private String commandSent() {
    return commandSent(remoteDatabase);
  }

  /**
   * The FIRST statement the call under test sent. Several of these methods follow their DDL with a schema reload,
   * whose SELECT would otherwise be what a "last command" captor answered with.
   */
  private String commandSent(final RemoteDatabase db) {
    final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
    verify(db, atLeastOnce()).command(anyString(), captor.capture());
    return captor.getAllValues().getFirst();
  }

  private Statement parse(final String sql) {
    return new SQLAntlrParser(null).parse(sql);
  }
}
