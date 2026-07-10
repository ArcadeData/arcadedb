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

import com.arcadedb.engine.Bucket;
import com.arcadedb.schema.*;
import com.arcadedb.server.BaseGraphServerTest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class RemoteSchemaIT extends BaseGraphServerTest {
  private static final String DATABASE_NAME = "remote-database";

  @Override
  protected boolean isCreateDatabases() {
    return false;
  }

  @Test
  void documentType() throws Exception {
    testEachServer(serverIndex -> {
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
  void vertexType() throws Exception {
    testEachServer(serverIndex -> {
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
  void edgeType() throws Exception {
    testEachServer(serverIndex -> {
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

  /**
   * Issue #4552: getBuckets() / getBucketByName() must trigger the lazy schema load instead of
   * throwing a NullPointerException on a fresh RemoteDatabase whose schema has not been loaded yet.
   */
  @Test
  void bucketAccessorsLoadSchemaLazily() throws Exception {
    testEachServer(serverIndex -> {
      // FRESH RemoteDatabase: schema (and the buckets map) is not loaded yet.
      try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {

        // Before the fix this threw NullPointerException because the buckets map was null.
        assertThatNoException().isThrownBy(() -> database.getSchema().getBuckets());
      }

      // Now create a type so we have a known bucket, then verify getBucketByName works on a fresh instance.
      try (final RemoteDatabase database = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
          BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
        final DocumentType type = database.getSchema().createDocumentType("BucketDoc");
        final String bucketName = type.getBuckets(false).getFirst().getName();

        // Fresh instance again to make sure the accessor itself loads the schema.
        try (final RemoteDatabase fresh = new RemoteDatabase("127.0.0.1", 2480 + serverIndex, DATABASE_NAME, "root",
            BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS)) {
          final Bucket bucket = fresh.getSchema().getBucketByName(bucketName);
          assertThat(bucket).isNotNull();
          assertThat(bucket.getName()).isEqualTo(bucketName);
          assertThat(fresh.getSchema().getBuckets()).isNotEmpty();
        }

        database.getSchema().dropType("BucketDoc");
      }
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
