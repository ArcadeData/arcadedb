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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

/**
 * Issue #7335: the four type builders on {@link RemoteSchema} used to throw a bare
 * {@link UnsupportedOperationException} with no message, leaving the caller with a dead end that named
 * neither the reason nor the alternative.
 * <p>
 * Each test asserts on the message TEXT, not merely on the exception type: a type-only assertion passes
 * against the pre-fix code and therefore proves nothing. The SQL alternative each message names is a real,
 * working remote path - {@code RemoteSchema} itself issues {@code create document/vertex/edge type} over the
 * wire, and {@code CREATE TIMESERIES TYPE} is exercised remotely by the gRPC and server integration tests.
 * <p>
 * The database is mocked, following the sibling {@code RemoteSchemaTest}: the builders throw before
 * dereferencing it, and a real {@link RemoteDatabase} cannot be used because its superclass constructor calls
 * {@code requestClusterConfiguration()}, which performs HTTP I/O against the host and port it is given.
 */
class Issue7335RemoteSchemaBuilderMessageTest {

  private RemoteSchema schema;

  @BeforeEach
  void setUp() {
    schema = new RemoteSchema(mock(RemoteDatabase.class));
  }

  @Test
  void buildDocumentTypeNamesTheSqlAlternative() {
    assertThatThrownBy(() -> schema.buildDocumentType())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("buildDocumentType()")
        .hasMessageContaining("not supported")
        .hasMessageContaining("CREATE DOCUMENT TYPE");
  }

  @Test
  void buildVertexTypeNamesTheSqlAlternative() {
    assertThatThrownBy(() -> schema.buildVertexType())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("buildVertexType()")
        .hasMessageContaining("not supported")
        .hasMessageContaining("CREATE VERTEX TYPE");
  }

  @Test
  void buildEdgeTypeNamesTheSqlAlternative() {
    assertThatThrownBy(() -> schema.buildEdgeType())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("buildEdgeType()")
        .hasMessageContaining("not supported")
        .hasMessageContaining("CREATE EDGE TYPE");
  }

  /**
   * Unlike the other three, {@code buildTimeSeriesType()} is not deprecated: it is current API, so this is the
   * message users are most likely to hit.
   */
  @Test
  void buildTimeSeriesTypeNamesTheSqlAlternative() {
    assertThatThrownBy(() -> schema.buildTimeSeriesType())
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("buildTimeSeriesType()")
        .hasMessageContaining("not supported")
        .hasMessageContaining("CREATE TIMESERIES TYPE");
  }
}
