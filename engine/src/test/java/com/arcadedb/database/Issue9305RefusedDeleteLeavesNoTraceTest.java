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
package com.arcadedb.database;

import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9305: {@code DELETE_RECORD} was checked only by {@code LocalBucket.deleteRecord}, the LAST step of a delete, after the
 * index entries, the EXTERNAL values and the vertex/edge links had already been removed. A caller owning its transaction that
 * carried on after the refusal committed a half-applied delete. The user below resolves permissions as the server does: the
 * grant map is keyed on the type's INVOLVED buckets, every other file id is default-allow. Since #9637 the involved buckets
 * include the paired {@code _ext} bucket too, so the refusal is now raised on the primary bucket before any cleanup starts AND
 * would be raised again on the paired bucket.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9305RefusedDeleteLeavesNoTraceTest {
  private static final String PATH = "target/databases/Issue9305RefusedDeleteLeavesNoTraceTest";
  private static final String BLOB = "x".repeat(200);

  private DatabaseFactory factory;
  private Database        database;

  @BeforeEach
  void setUp() {
    factory = new DatabaseFactory(PATH).setSecurity(db -> {
    });
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
  }

  @AfterEach
  void tearDown() {
    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    database.drop();
    factory.close();
  }

  @Test
  void refusedDocumentDeleteKeepsIndexAndExternalValues() {
    final DocumentType type = database.getSchema().createDocumentType("T");
    type.createProperty("id", Type.STRING);
    type.createProperty("blob", Type.STRING).setExternal(true);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "T", "id");

    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("T").set("id", "a").set("blob", BLOB).save().getIdentity());

    bindUserRefusingDeleteOn(type);

    database.begin();
    final Throwable refused = catchThrowable(() -> database.deleteRecord(rid[0].asDocument()));
    assertThat(refused).isInstanceOf(SecurityException.class);
    database.commit();

    assertThat(database.countType("T", false)).isEqualTo(1);
    assertThat(externalCount(type)).isEqualTo(1);
    final IndexCursor cursor = database.getSchema().getIndexByName("T[id]").get(new Object[] { "a" });
    assertThat(cursor.hasNext()).isTrue();
    assertThat((String) rid[0].asDocument().getString("blob")).isEqualTo(BLOB);
  }

  @Test
  void refusedVertexDeleteKeepsEdgesAndIndex() {
    final DocumentType v = database.getSchema().createVertexType("V");
    v.createProperty("id", Type.STRING);
    database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "V", "id");
    database.getSchema().createEdgeType("E");

    final RID[] ids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").set("id", "a").save();
      final MutableVertex b = database.newVertex("V").set("id", "b").save();
      a.newEdge("E", b);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
    });

    bindUserRefusingDeleteOn(v);

    database.begin();
    assertThat(catchThrowable(() -> database.deleteRecord(ids[0].asVertex()))).isInstanceOf(SecurityException.class);
    database.commit();

    assertThat(database.countType("E", false)).isEqualTo(1);
    assertThat(ids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
    assertThat(database.getSchema().getIndexByName("V[id]").get(new Object[] { "a" }).hasNext()).isTrue();
  }

  @Test
  void refusedEdgeDeleteKeepsBothEndpointsLinked() {
    database.getSchema().createVertexType("V");
    final DocumentType e = database.getSchema().createEdgeType("E");

    final RID[] ids = new RID[3];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
      ids[2] = a.newEdge("E", b).getIdentity();
    });

    bindUserRefusingDeleteOn(e);

    database.begin();
    assertThat(catchThrowable(() -> database.deleteRecord(ids[2].asEdge()))).isInstanceOf(SecurityException.class);
    database.commit();

    assertThat(database.countType("E", false)).isEqualTo(1);
    assertThat(ids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
  }

  /** The refusal comes from a bucket the vertex delete reaches only AFTER it started (its edge): the commit must not publish that. */
  @Test
  void refusalRaisedMidDeleteDoomsTheTransaction() {
    final DocumentType v = database.getSchema().createVertexType("V");
    final DocumentType e = database.getSchema().createEdgeType("E");

    final RID[] ids = new RID[2];
    database.transaction(() -> {
      final MutableVertex a = database.newVertex("V").save();
      final MutableVertex b = database.newVertex("V").save();
      a.newEdge("E", b);
      ids[0] = a.getIdentity();
      ids[1] = b.getIdentity();
    });

    bindUserRefusingDeleteOn(e);

    database.begin();
    assertThat(catchThrowable(() -> database.deleteRecord(ids[0].asVertex()))).isInstanceOf(SecurityException.class);
    assertThat(catchThrowable(database::commit)).isNotNull();
    if (database.isTransactionActive())
      database.rollback();

    assertThat(database.countType("V", false)).isEqualTo(2);
    assertThat(database.countType("E", false)).isEqualTo(1);
    assertThat(ids[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1);
    assertThat(ids[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1);
  }

  @Test
  void grantedDeleteStillWorks() {
    final DocumentType type = database.getSchema().createDocumentType("T");
    type.createProperty("blob", Type.STRING).setExternal(true);
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newDocument("T").set("blob", BLOB).save().getIdentity());

    database.transaction(() -> database.deleteRecord(rid[0].asDocument()));

    assertThat(database.countType("T", false)).isZero();
    assertThat(externalCount(type)).isZero();
  }

  private long externalCount(final DocumentType type) {
    long total = 0;
    for (final Bucket bucket : type.getBuckets(false)) {
      final Integer externalId = ((LocalDocumentType) type).getExternalBucketIdFor(bucket.getFileId());
      assertThat(externalId).isNotNull();
      total += ((LocalBucket) database.getSchema().getBucketById(externalId)).count();
    }
    return total;
  }

  /** Same resolution as ServerSecurityDatabaseUser: only the type's involved bucket ids are listed, the rest is default-allow. */
  private void bindUserRefusingDeleteOn(final DocumentType type) {
    final Set<Integer> gated = new HashSet<>();
    for (final Bucket bucket : type.getInvolvedBuckets())
      gated.add(bucket.getFileId());

    DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
      @Override
      public String getName() {
        return "nodelete";
      }

      @Override
      public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
        return true;
      }

      @Override
      public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
        return access != ACCESS.DELETE_RECORD || !gated.contains(fileId);
      }

      @Override
      public boolean requestAccessOnType(final String typeName, final ACCESS access) {
        return true;
      }

      @Override
      public long getResultSetLimit() {
        return -1L;
      }

      @Override
      public long getReadTimeout() {
        return -1L;
      }
    });
  }
}
