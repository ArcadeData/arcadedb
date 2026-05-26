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
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
}
