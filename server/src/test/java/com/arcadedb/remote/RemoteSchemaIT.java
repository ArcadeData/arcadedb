/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.arcadedb.remote;

import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;
import com.arcadedb.server.BaseGraphServerTest;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class RemoteSchemaIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  public void documentType() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      assertThat(database.getSchema().existsType("Document")).isFalse();
      DocumentType type = database.getSchema().createDocumentType("Document");
      assertThat(type).isNotNull();
      assertThat(type.getName()).isEqualTo("Document");
      assertThat(database.getSchema().existsType("Document")).isTrue();

      type.createProperty("a", Type.STRING);
      assertThat(type.existsProperty("a")).isTrue();
      assertThat(type.existsProperty("zz")).isFalse();

      type.createProperty("b", Type.LIST, "STRING");
      assertThat(type.existsProperty("b")).isTrue();

      database.getSchema().dropType("Document");
    });
  }

  @Test
  public void vertexType() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      assertThat(database.getSchema().existsType("Vertex")).isFalse();
      VertexType type = database.getSchema().createVertexType("Vertex");
      assertThat(type).isNotNull();
      assertThat(type.getName()).isEqualTo("Vertex");
      assertThat(database.getSchema().existsType("Vertex")).isTrue();

      VertexType type2 = database.getSchema().createVertexType("Vertex2");

      Property prop2 = type2.createProperty("b", Type.LIST, "STRING");
      assertThat(prop2.getName()).isEqualTo("b");
      assertThat(prop2.getType()).isEqualTo(Type.LIST);
      assertThat(prop2.getOfType()).isEqualTo("STRING");

      type2.addSuperType("Vertex");
      assertThat(type2.isSubTypeOf("Vertex")).isTrue();

      VertexType type3 = database.getSchema().createVertexType("Vertex3");
      Property prop3 = type3.createProperty("c", Type.LIST, "INTEGER");
      assertThat(prop3.getName()).isEqualTo("c");
      assertThat(prop3.getType()).isEqualTo(Type.LIST);
      assertThat(prop3.getOfType()).isEqualTo("INTEGER");

      type3.addSuperType("Vertex2");
      assertThat(type3.isSubTypeOf("Vertex")).isTrue();
      assertThat(type3.isSubTypeOf("Vertex2")).isTrue();

      assertThat(database.getSchema().getTypes()).hasSize(3);

      assertThat(database.getSchema().getType("Vertex")).isNotNull();
      assertThat(database.getSchema().getType("Vertex2")).isNotNull();
      assertThat(database.getSchema().getType("Vertex3")).isNotNull();

      database.getSchema().dropType("Vertex3");
      database.getSchema().dropType("Vertex2");
      database.getSchema().dropType("Vertex");
    });
  }

  @Test
  public void edgeType() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      assertThat(database.getSchema().existsType("Edge")).isFalse();
      EdgeType type = database.getSchema().createEdgeType("Edge");
      assertThat(type).isNotNull();
      assertThat(type.getName()).isEqualTo("Edge");
      assertThat(database.getSchema().existsType("Edge")).isTrue();
      database.getSchema().dropType("Edge");
    });
  }

  @BeforeEach
  public void beginTest() {
    super.beginTest();
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (!server.exists(DATABASE_NAME))
      server.create(DATABASE_NAME);
  }

  @AfterEach
  public void endTest() {
    final RemoteServer server = new RemoteServer("127.0.0.1", 2480, "root", BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);
    if (server.exists(DATABASE_NAME))
      server.drop(DATABASE_NAME);
    super.endTest();
  }
}
