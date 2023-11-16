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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

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

      Assertions.assertFalse(database.getSchema().existsType("Document"));
      DocumentType type = database.getSchema().createDocumentType("Document");
      Assertions.assertNotNull(type);
      Assertions.assertEquals(type.getName(), "Document");
      Assertions.assertTrue(database.getSchema().existsType("Document"));

      type.createProperty("a", Type.STRING);
      Assertions.assertTrue(type.existsProperty("a"));
      Assertions.assertFalse(type.existsProperty("zz"));

      type.createProperty("b", Type.LIST, "STRING");
      Assertions.assertTrue(type.existsProperty("b"));

      database.getSchema().dropType("Document");
    });
  }

  @Test
  public void vertexType() throws Exception {
    testEachServer((serverIndex) -> {
      final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS);

      Assertions.assertFalse(database.getSchema().existsType("Vertex"));
      VertexType type = database.getSchema().createVertexType("Vertex");
      Assertions.assertNotNull(type);
      Assertions.assertEquals(type.getName(), "Vertex");
      Assertions.assertTrue(database.getSchema().existsType("Vertex"));

      VertexType type2 = database.getSchema().createVertexType("Vertex2");

      Property prop2 = type2.createProperty("b", Type.LIST, "STRING");
      Assertions.assertEquals("b", prop2.getName());
      Assertions.assertEquals(Type.LIST, prop2.getType());
      Assertions.assertEquals("STRING", prop2.getOfType());

      type2.addSuperType("Vertex");
      Assertions.assertTrue(type2.isSubTypeOf("Vertex"));

      VertexType type3 = database.getSchema().createVertexType("Vertex3");
      Property prop3 = type3.createProperty("c", Type.LIST, "INTEGER");
      Assertions.assertEquals("c", prop3.getName());
      Assertions.assertEquals(Type.LIST, prop3.getType());
      Assertions.assertEquals("INTEGER", prop3.getOfType());

      type3.addSuperType("Vertex2");
      Assertions.assertTrue(type3.isSubTypeOf("Vertex"));
      Assertions.assertTrue(type3.isSubTypeOf("Vertex2"));

      Assertions.assertEquals(3, database.getSchema().getTypes().size());

      Assertions.assertNotNull(database.getSchema().getType("Vertex"));
      Assertions.assertNotNull(database.getSchema().getType("Vertex2"));
      Assertions.assertNotNull(database.getSchema().getType("Vertex3"));

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

      Assertions.assertFalse(database.getSchema().existsType("Edge"));
      EdgeType type = database.getSchema().createEdgeType("Edge");
      Assertions.assertNotNull(type);
      Assertions.assertEquals(type.getName(), "Edge");
      Assertions.assertTrue(database.getSchema().existsType("Edge"));
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
