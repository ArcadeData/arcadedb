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

import com.arcadedb.database.Identifiable;
import com.arcadedb.database.RID;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RemoteVertexTest {

  private RemoteDatabase mockDatabase;
  private Vertex         mockVertex;
  private Identifiable   mockToVertex;
  private RemoteVertex   remoteVertex;

  @BeforeEach
  void setUp() {
    mockDatabase = mock(RemoteDatabase.class);
    mockVertex = mock(Vertex.class);
    mockToVertex = mock(Identifiable.class);

    when(mockVertex.getIdentity()).thenReturn(new RID(1, 0));
    when(mockToVertex.getIdentity()).thenReturn(new RID(2, 0));

    // Return an incomplete result so command() is called but edge construction fails.
    // Mockito records the command() call before the exception, so ArgumentCaptor still works.
    final Result mockResult = mock(Result.class);
    when(mockResult.getEdge()).thenReturn(Optional.empty());
    final ResultSet mockResultSet = mock(ResultSet.class);
    when(mockResultSet.next()).thenReturn(mockResult);

    when(mockDatabase.command(eq("sql"), any(String.class), any(Map.class))).thenReturn(mockResultSet);

    remoteVertex = new RemoteVertex(mockVertex, mockDatabase);
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgeDoesNotInterpolateStringPropertyIntoSql() {
    // String values with apostrophes must be passed as SQL parameters, not interpolated.
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, "lastName", "O'Brien"))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).doesNotContain("O'Brien");
    assertThat(sqlCaptor.getValue()).contains(":p0");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsEntry(":p0", "O'Brien");
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgePassesNonStringPropertyAsParameter() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, "score", 42))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).contains(":p0");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsEntry(":p0", 42);
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgePassesMultiplePropertiesAsParameters() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, "name", "O'Brien", "since", 2023))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    final String sql = sqlCaptor.getValue();
    assertThat(sql).doesNotContain("O'Brien");
    assertThat(sql).contains("`name` = :p0");
    assertThat(sql).contains("`since` = :p1");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsEntry(":p0", "O'Brien");
    assertThat(params).containsEntry(":p1", 2023);
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgeWithMapPropertiesUsesParameterBinding() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex, Map.of("label", "it's a test")))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).doesNotContain("it's a test");
    final Map<String, Object> params = (Map<String, Object>) paramsCaptor.getValue();
    assertThat(params).containsValue("it's a test");
  }

  @SuppressWarnings("unchecked")
  @Test
  void newEdgeWithNoPropertiesCallsCommandWithEmptyParams() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<Map> paramsCaptor = ArgumentCaptor.forClass(Map.class);

    assertThatThrownBy(() -> remoteVertex.newEdge("FriendOf", mockToVertex))
        .isInstanceOf(Exception.class);

    verify(mockDatabase).command(eq("sql"), sqlCaptor.capture(), paramsCaptor.capture());
    assertThat(sqlCaptor.getValue()).doesNotContain("set");
    assertThat(paramsCaptor.getValue()).isEmpty();
  }

  @Test
  void getVerticesWithLimitAndSkipEmitsSqlWithSkipLimit() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.hasNext()).thenReturn(false);
    when(mockDatabase.query(eq("sql"), sqlCaptor.capture())).thenReturn(emptyRs);

    remoteVertex.getVertices(Vertex.DIRECTION.OUT, 100, 50, "Knows");

    final String sql = sqlCaptor.getValue();
    assertThat(sql).contains("out(").contains("'Knows'");
    assertThat(sql).contains("SKIP 50");
    assertThat(sql).contains("LIMIT 100");
  }

  @Test
  void getVerticesDefaultUsesDefaultPageSize() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.hasNext()).thenReturn(false);
    when(mockDatabase.query(eq("sql"), sqlCaptor.capture())).thenReturn(emptyRs);

    // iterator is lazy - must consume it to trigger the first page fetch
    remoteVertex.getVertices(Vertex.DIRECTION.OUT, "Knows").iterator().hasNext();

    final String sql = sqlCaptor.getValue();
    assertThat(sql).doesNotContain("SKIP");
    assertThat(sql).contains("LIMIT " + RemoteVertex.DEFAULT_PAGE_SIZE);
  }

  @Test
  void getVerticesPagedFetchesMultiplePagesUntilLastPageIsSmaller() {
    // Two full pages of size 2, then one partial page of size 1 — iterator must call query 3 times.
    final Result r1 = vertexResult(1);
    final Result r2 = vertexResult(2);
    final Result r3 = vertexResult(3);
    final Result r4 = vertexResult(4);
    final Result r5 = vertexResult(5);

    final ResultSet page1 = resultSetOf(r1, r2);
    final ResultSet page2 = resultSetOf(r3, r4);
    final ResultSet page3 = resultSetOf(r5);

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.query(eq("sql"), sqlCaptor.capture()))
        .thenReturn(page1)
        .thenReturn(page2)
        .thenReturn(page3);

    final List<Vertex> collected = new ArrayList<>();
    for (final Vertex v : remoteVertex.getVerticesPaged(Vertex.DIRECTION.OUT, 2, "Knows"))
      collected.add(v);

    assertThat(collected).hasSize(5);

    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries).hasSize(3);
    // skip=0 is omitted (redundant); skip>0 is always emitted
    assertThat(queries.getFirst()).doesNotContain("SKIP").contains("LIMIT 2");
    assertThat(queries.get(1)).contains("SKIP 2").contains("LIMIT 2");
    assertThat(queries.get(2)).contains("SKIP 4").contains("LIMIT 2");
  }

  @Test
  void getEdgesWithLimitAndSkipEmitsSqlWithSkipLimit() {
    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    final ResultSet emptyRs = mock(ResultSet.class);
    when(emptyRs.hasNext()).thenReturn(false);
    when(mockDatabase.query(eq("sql"), sqlCaptor.capture())).thenReturn(emptyRs);

    remoteVertex.getEdges(Vertex.DIRECTION.BOTH, 200, 0, "Knows");

    final String sql = sqlCaptor.getValue();
    assertThat(sql).contains("bothE(").contains("'Knows'");
    assertThat(sql).doesNotContain("SKIP");
    assertThat(sql).contains("LIMIT 200");
  }

  @Test
  void getEdgesPagedFetchesMultiplePages() {
    final Result e1 = edgeResult(1);
    final Result e2 = edgeResult(2);

    final ResultSet page1 = edgeResultSetOf(e1, e2);
    final ResultSet page2 = edgeResultSetOf();

    final ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
    when(mockDatabase.query(eq("sql"), sqlCaptor.capture()))
        .thenReturn(page1)
        .thenReturn(page2);

    final List<Edge> collected = new ArrayList<>();
    for (final Edge e : remoteVertex.getEdgesPaged(Vertex.DIRECTION.OUT, 2))
      collected.add(e);

    assertThat(collected).hasSize(2);
    final List<String> queries = sqlCaptor.getAllValues();
    assertThat(queries.getFirst()).contains("outE(").doesNotContain("SKIP").contains("LIMIT 2");
    assertThat(queries.get(1)).contains("SKIP 2").contains("LIMIT 2");
  }

  // --- helpers ---

  private Result vertexResult(final int bucketPos) {
    final Vertex v = mock(Vertex.class);
    when(v.getIdentity()).thenReturn(new RID(1, bucketPos));
    final Result r = mock(Result.class);
    when(r.getVertex()).thenReturn(Optional.of(v));
    return r;
  }

  private Result edgeResult(final int bucketPos) {
    final Edge e = mock(Edge.class);
    when(e.getIdentity()).thenReturn(new RID(3, bucketPos));
    final Result r = mock(Result.class);
    when(r.getEdge()).thenReturn(Optional.of(e));
    return r;
  }

  private ResultSet resultSetOf(final Result... results) {
    final ResultSet rs = mock(ResultSet.class);
    final boolean[] hasNextAnswers = new boolean[results.length + 1];
    for (int i = 0; i < results.length; i++)
      hasNextAnswers[i] = true;
    hasNextAnswers[results.length] = false;

    final Boolean[] hasNextBoxed = new Boolean[hasNextAnswers.length];
    for (int i = 0; i < hasNextAnswers.length; i++)
      hasNextBoxed[i] = hasNextAnswers[i];

    if (hasNextBoxed.length > 1) {
      final Boolean first = hasNextBoxed[0];
      final Boolean[] rest = Arrays.copyOfRange(hasNextBoxed, 1, hasNextBoxed.length);
      when(rs.hasNext()).thenReturn(first, rest);
    } else {
      when(rs.hasNext()).thenReturn(false);
    }

    if (results.length > 0) {
      final Result first = results[0];
      final Result[] rest = Arrays.copyOfRange(results, 1, results.length);
      when(rs.next()).thenReturn(first, rest);
    }
    return rs;
  }

  private ResultSet edgeResultSetOf(final Result... results) {
    return resultSetOf(results);
  }
}
