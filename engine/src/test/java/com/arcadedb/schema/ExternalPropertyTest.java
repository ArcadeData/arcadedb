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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.MutableEmbeddedDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies the EXTERNAL property storage feature: when a property is flagged EXTERNAL, its value lives in a paired
 * external bucket keyed by the same record's RID, while the main record only carries a TYPE_EXTERNAL pointer.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class ExternalPropertyTest extends TestHelper {

  @Test
  void flagPersistsAcrossReopen() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("blob", Type.STRING).setExternal(true);

    assertThat(type.getProperty("blob").isExternal()).isTrue();
    assertThat(type.getProperty("name").isExternal()).isFalse();

    database.close();
    database = factory.open();

    final DocumentType reloaded = database.getSchema().getType("Doc");
    assertThat(reloaded.getProperty("blob").isExternal()).isTrue();
    assertThat(reloaded.getProperty("name").isExternal()).isFalse();
  }

  @Test
  void valueRoundTripDocument() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("blob", Type.STRING).setExternal(true);

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc")
          .set("name", "alice")
          .set("blob", "the quick brown fox jumps over the lazy dog");
      d.save();
      saved[0] = d.getIdentity();
    });

    // Re-read from disk to be sure we exercise the deserialization path, not an in-memory cache hit.
    database.close();
    database = factory.open();

    final MutableDocument loaded = (MutableDocument) database.lookupByRID(saved[0], true).asDocument().modify();
    assertThat(loaded.getString("name")).isEqualTo("alice");
    assertThat(loaded.getString("blob")).isEqualTo("the quick brown fox jumps over the lazy dog");
  }

  @Test
  void valueRoundTripVertexLargeArray() {
    final VertexType type = database.getSchema().createVertexType("V");
    type.createProperty("name", Type.STRING);
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS).setExternal(true);

    final float[] embedding = new float[4096];
    for (int i = 0; i < embedding.length; i++)
      embedding[i] = (float) Math.sin(i);

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V")
          .set("name", "v1")
          .set("embedding", embedding);
      v.save();
      saved[0] = v.getIdentity();
    });

    database.close();
    database = factory.open();

    final var loaded = database.lookupByRID(saved[0], true).asVertex();
    assertThat(loaded.getString("name")).isEqualTo("v1");
    final Object readBack = loaded.get("embedding");
    assertThat(readBack).isInstanceOf(float[].class);
    final float[] readBackArr = (float[]) readBack;
    assertThat(readBackArr).hasSize(embedding.length);
    for (int i = 0; i < embedding.length; i++)
      assertThat(readBackArr[i]).as("position %d", i).isEqualTo(embedding[i]);
  }

  @Test
  void pairedExternalBucketIsCreatedAndMarkedSystem() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    // For each primary bucket of the type there must be a paired external bucket marked EXTERNAL_PROPERTY.
    boolean foundAtLeastOne = false;
    for (final var primaryBucket : type.getBuckets(false)) {
      final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primaryBucket.getFileId());
      assertThat(extId).as("external bucket id for primary %d", primaryBucket.getFileId()).isNotNull();
      final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);
      assertThat(external.getPurpose()).isEqualTo(LocalBucket.Purpose.EXTERNAL_PROPERTY);
      assertThat(external.getName()).isEqualTo(primaryBucket.getName() + "_ext");
      foundAtLeastOne = true;
    }
    assertThat(foundAtLeastOne).as("type should have at least one primary bucket").isTrue();

    // Type's regular buckets() list must NOT include the external buckets.
    for (final var b : type.getBuckets(false))
      assertThat(((LocalBucket) b).getPurpose()).isEqualTo(LocalBucket.Purpose.PRIMARY);
  }

  @Test
  void inheritancePropagatesPairedExternalBucketsToSubtype() {
    final DocumentType parent = database.getSchema().createDocumentType("Parent");
    parent.createProperty("blob", Type.STRING).setExternal(true);

    final DocumentType child = database.getSchema().createDocumentType("Child");
    child.addSuperType("Parent");

    // Each of Child's primary buckets must have its own paired external bucket, even though the EXTERNAL property
    // is inherited (not declared on Child directly). Records of Child live in Child's primary buckets.
    for (final var primaryBucket : child.getBuckets(false)) {
      final Integer extId = ((LocalDocumentType) child).getExternalBucketIdFor(primaryBucket.getFileId());
      assertThat(extId).as("external bucket id for child primary %d", primaryBucket.getFileId()).isNotNull();
    }

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Child").set("blob", "child blob");
      d.save();
      saved[0] = d.getIdentity();
    });

    database.close();
    database = factory.open();

    final MutableDocument loaded = (MutableDocument) database.lookupByRID(saved[0], true).asDocument().modify();
    assertThat(loaded.getString("blob")).isEqualTo("child blob");
  }

  @Test
  void updateOfExternalProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc").set("blob", "v1");
      d.save();
      saved[0] = d.getIdentity();
    });

    database.transaction(() -> {
      final MutableDocument d = (MutableDocument) database.lookupByRID(saved[0], true).asDocument().modify();
      d.set("blob", "v2-the-second-revision");
      d.save();
    });

    database.close();
    database = factory.open();

    final MutableDocument loaded = (MutableDocument) database.lookupByRID(saved[0], true).asDocument().modify();
    assertThat(loaded.getString("blob")).isEqualTo("v2-the-second-revision");
  }

  @Test
  void deleteCascadesToExternalRecord() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc").set("blob", "to-be-deleted");
      d.save();
      saved[0] = d.getIdentity();
    });

    // Verify the external bucket has at least one record before delete.
    final Integer extBucketId = ((LocalDocumentType) type).getExternalBucketIdFor(saved[0].getBucketId());
    final LocalBucket externalBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extBucketId);
    final long extCountBefore = externalBucket.count();
    assertThat(extCountBefore).isGreaterThanOrEqualTo(1L);

    database.transaction(() -> {
      database.lookupByRID(saved[0], true).asDocument().delete();
    });

    final long extCountAfter = externalBucket.count();
    assertThat(extCountAfter).as("external record should be deleted by cascade").isEqualTo(extCountBefore - 1L);
  }

  @Test
  void directWriteToExternalBucketIsRejected() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final var primaryBucket = type.getBuckets(false).getFirst();
    final Integer extBucketId = ((LocalDocumentType) type).getExternalBucketIdFor(primaryBucket.getFileId());
    final LocalBucket externalBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extBucketId);

    // The Java path that resolves a bucket by name must reject an external bucket. Build a fresh document and try
    // to route it to the external bucket via Database.createRecord(record, bucketName).
    final MutableDocument fresh = database.newDocument("Doc").set("blob", "x");
    org.assertj.core.api.Assertions.assertThatThrownBy(() ->
        database.transaction(() ->
            ((com.arcadedb.database.DatabaseInternal) database).createRecord(fresh, externalBucket.getName())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("internal");

    // SQL INSERT INTO bucket:<external> is also rejected (by the SQL planner, since the bucket has no associated
    // type). Different error message but functionally equivalent: the user cannot target the bucket.
    org.assertj.core.api.Assertions.assertThatThrownBy(() ->
        database.transaction(() ->
            database.command("sql", "INSERT INTO bucket:" + externalBucket.getName() + " SET x = 1")))
        .isInstanceOf(Exception.class);
  }

  @Test
  void sqlDdlCreateAndAlterExternal() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.blob STRING (EXTERNAL true)");
    });
    assertThat(database.getSchema().getType("Doc").getProperty("blob").isExternal()).isTrue();

    database.transaction(() -> {
      database.command("sql", "ALTER PROPERTY Doc.blob EXTERNAL false");
    });
    assertThat(database.getSchema().getType("Doc").getProperty("blob").isExternal()).isFalse();
  }

  @Test
  void alterToExternalRelocatesOnNextWrite() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING); // inline initially

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc").set("blob", "starts-inline");
      d.save();
      saved[0] = d.getIdentity();
    });

    // Flip to EXTERNAL. Existing record's bytes are not rewritten yet.
    type.getProperty("blob").setExternal(true);

    // Read still returns the inline value: deserializer doesn't see TYPE_EXTERNAL in the OLD bytes.
    var loaded = database.lookupByRID(saved[0], true).asDocument();
    assertThat(loaded.getString("blob")).isEqualTo("starts-inline");

    // Update the property. Re-serialize must route through the external bucket now.
    database.transaction(() -> {
      final MutableDocument m = (MutableDocument) database.lookupByRID(saved[0], true).asDocument().modify();
      m.set("blob", "now-external");
      m.save();
    });

    database.close();
    database = factory.open();

    final var reloaded = database.lookupByRID(saved[0], true).asDocument();
    assertThat(reloaded.getString("blob")).isEqualTo("now-external");

    // The external bucket should hold the new value.
    final Integer extBucketId = ((LocalDocumentType) database.getSchema().getType("Doc"))
        .getExternalBucketIdFor(saved[0].getBucketId());
    assertThat(extBucketId).isNotNull();
    final LocalBucket externalBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extBucketId);
    assertThat(externalBucket.count()).isGreaterThanOrEqualTo(1L);
  }

  @Test
  void indexLookupOnExternalProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING).setExternal(true);
    type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "name");

    database.transaction(() -> {
      for (int i = 0; i < 50; i++)
        database.newDocument("Doc").set("name", "user-" + i).save();
    });

    final ResultSet rs = database.query("sql", "SELECT FROM Doc WHERE name = 'user-37'");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("name")).isEqualTo("user-37");
    assertThat(rs.hasNext()).isFalse();
  }

  @Test
  void schemaBucketsViewExposesPurposeColumn() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);

    final ResultSet rs = database.query("sql", "SELECT name, purpose FROM schema:buckets");
    boolean foundPrimary = false;
    boolean foundExternal = false;
    while (rs.hasNext()) {
      final var row = rs.next();
      final String name = row.getProperty("name");
      if (name.equals(primary.getName())) {
        foundPrimary = true;
        assertThat((String) row.getProperty("purpose")).isEqualTo("PRIMARY");
      } else if (name.equals(external.getName())) {
        foundExternal = true;
        assertThat((String) row.getProperty("purpose")).isEqualTo("EXTERNAL_PROPERTY");
      }
    }
    assertThat(foundPrimary).as("schema:buckets should list the primary bucket").isTrue();
    assertThat(foundExternal).as("schema:buckets should list the external bucket").isTrue();

    // The Studio buckets tab uses this exact WHERE filter to hide internal buckets. Verify it works on
    // schema:buckets and excludes the EXTERNAL_PROPERTY bucket but still includes the primary one.
    final ResultSet filtered = database.query("sql",
        "SELECT name, purpose FROM schema:buckets WHERE purpose = 'PRIMARY' OR purpose IS NULL");
    final java.util.Set<String> names = new java.util.HashSet<>();
    while (filtered.hasNext())
      names.add(filtered.next().getProperty("name"));
    assertThat(names).contains(primary.getName());
    assertThat(names).doesNotContain(external.getName());
  }

  @Test
  void rebuildTypeMovesInlineToExternal() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING); // inline initially

    final int n = 25;
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        database.newDocument("Doc").set("blob", "payload-" + i).save();
    });

    // Flip the flag, rebuild.
    type.getProperty("blob").setExternal(true);

    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "REBUILD TYPE Doc");
      assertThat(rs.hasNext()).isTrue();
      final var row = rs.next();
      assertThat((Long) row.getProperty("recordsRebuilt")).isEqualTo((long) n);
    });

    // After rebuild, the external bucket should hold one record per Doc record.
    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);
    assertThat(external.count()).isEqualTo((long) n);

    // Values still readable.
    final ResultSet rs = database.query("sql", "SELECT blob FROM Doc ORDER BY blob");
    int counted = 0;
    while (rs.hasNext()) {
      final String val = rs.next().getProperty("blob");
      assertThat(val).startsWith("payload-");
      counted++;
    }
    assertThat(counted).isEqualTo(n);
  }

  @Test
  void rebuildTypeReversesExternalToInlineAndCleansOrphans() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final int n = 15;
    database.transaction(() -> {
      for (int i = 0; i < n; i++)
        database.newDocument("Doc").set("blob", "ext-" + i).save();
    });

    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);
    final String externalBucketName = external.getName();
    assertThat(external.count()).isEqualTo((long) n);

    // Flip OFF and rebuild. REBUILD manages its own transaction; wrapping it in a caller transaction would
    // defer the deferred-update flush past the in-statement reclaim check.
    type.getProperty("blob").setExternal(false);
    database.command("sql", "REBUILD TYPE Doc");

    // Values must still be readable inline.
    final ResultSet rs = database.query("sql", "SELECT blob FROM Doc");
    int counted = 0;
    while (rs.hasNext()) {
      assertThat((String) rs.next().getProperty("blob")).startsWith("ext-");
      counted++;
    }
    assertThat(counted).isEqualTo(n);

    // Bucket reclaim: the now-empty paired external bucket is dropped (no accumulation across toggle cycles)
    // and the type's external-bucket map is cleared so schema.json no longer carries an externalBuckets key.
    assertThat(((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId()))
        .as("paired external bucket id should be cleared from the type after REBUILD")
        .isNull();
    assertThat(((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId, false))
        .as("paired external bucket file should be dropped").isNull();
    assertThat(database.getSchema().existsBucket(externalBucketName))
        .as("schema should no longer expose the dropped external bucket").isFalse();
  }

  /**
   * REBUILD TYPE inside a caller-supplied transaction cannot reclaim now-empty paired buckets (the queued
   * record updates haven't flushed). The result row must surface a {@code warning} so the operator knows to
   * re-run REBUILD outside a transaction.
   */
  @Test
  void rebuildTypeWarnsWhenInsideCallerTransactionAndExternalBucketsRemain() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("Doc").set("blob", "v-" + i).save();
    });
    type.getProperty("blob").setExternal(false);

    database.begin();
    final ResultSet rs = database.command("sql", "REBUILD TYPE Doc");
    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat((String) row.getProperty("warning"))
        .as("warning must be set so the operator knows reclaim was deferred")
        .contains("paired external buckets were NOT reclaimed");
    database.commit();

    // After committing the caller tx, re-running REBUILD outside any transaction must complete the reclaim.
    final ResultSet rs2 = database.command("sql", "REBUILD TYPE Doc");
    assertThat(rs2.hasNext()).isTrue();
    final var row2 = rs2.next();
    assertThat((String) row2.getProperty("warning")).as("second run owns the tx and reclaims; no warning").isNull();
    assertThat(((LocalDocumentType) database.getSchema().getType("Doc")).hasExternalBuckets())
        .as("paired external buckets should be dropped after the second REBUILD").isFalse();
  }

  @Test
  void rebuildTypePolymorphicWalksSubtypes() {
    final DocumentType parent = database.getSchema().createDocumentType("Parent");
    parent.createProperty("blob", Type.STRING);
    final DocumentType child = database.getSchema().createDocumentType("Child");
    child.addSuperType("Parent");

    database.transaction(() -> {
      database.newDocument("Parent").set("blob", "p1").save();
      database.newDocument("Child").set("blob", "c1").save();
      database.newDocument("Child").set("blob", "c2").save();
    });

    // Toggle EXTERNAL on the inherited property and rebuild polymorphically.
    parent.getProperty("blob").setExternal(true);
    database.transaction(() -> {
      final ResultSet rs = database.command("sql", "REBUILD TYPE Parent POLYMORPHIC");
      assertThat(rs.hasNext()).isTrue();
      assertThat((Long) rs.next().getProperty("recordsRebuilt")).isEqualTo(3L);
    });

    // Both Parent and Child external buckets should now hold their respective records.
    final var parentBucket = parent.getBuckets(false).getFirst();
    final Integer parentExtId = ((LocalDocumentType) parent).getExternalBucketIdFor(parentBucket.getFileId());
    final var childBucket = child.getBuckets(false).getFirst();
    final Integer childExtId = ((LocalDocumentType) child).getExternalBucketIdFor(childBucket.getFileId());
    final var localSchema = (LocalSchema) database.getSchema().getEmbedded();
    assertThat(localSchema.getBucketById(parentExtId).count()).isEqualTo(1L);
    assertThat(localSchema.getBucketById(childExtId).count()).isEqualTo(2L);
  }

  @Test
  void schemaTypesViewExposesExternalFlagAndPairing() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("blob", Type.STRING).setExternal(true);

    final ResultSet rs = database.query("sql", "SELECT FROM schema:types WHERE name = 'Doc'");
    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();

    // Per-property external flag (only emitted when true).
    final var properties = (java.util.List<?>) row.getProperty("properties");
    assertThat(properties).isNotNull();
    boolean foundBlobAsExternal = false;
    boolean foundNameWithoutFlag = false;
    for (final Object propObj : properties) {
      final var prop = (com.arcadedb.query.sql.executor.Result) propObj;
      final String name = prop.getProperty("name");
      if ("blob".equals(name)) {
        assertThat((Boolean) prop.getProperty("external")).isTrue();
        foundBlobAsExternal = true;
      } else if ("name".equals(name)) {
        assertThat((Boolean) prop.getProperty("external")).isNull();
        foundNameWithoutFlag = true;
      }
    }
    assertThat(foundBlobAsExternal).isTrue();
    assertThat(foundNameWithoutFlag).isTrue();

    // Type-level externalBuckets mapping (primaryBucketName -> externalBucketName).
    @SuppressWarnings("unchecked")
    final java.util.Map<String, String> extMap = (java.util.Map<String, String>) row.getProperty("externalBuckets");
    assertThat(extMap).isNotNull().isNotEmpty();
    final String primaryName = type.getBuckets(false).getFirst().getName();
    assertThat(extMap).containsKey(primaryName);
    assertThat(extMap.get(primaryName)).endsWith("_ext");
  }

  @Test
  void externalBucketUsesLargerDefaultPageSize() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    // Primary bucket uses the standard 64KB page; external bucket uses the heavier 256KB page so multi-KB
    // payloads (vectors, big strings) fit in a single page rather than overflowing into the chunk-chain path.
    final LocalBucket primary = (LocalBucket) type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);

    assertThat(primary.getPageSize()).isEqualTo(65_536);
    assertThat(external.getPageSize()).isEqualTo(262_144);
    assertThat(external.getPageSize()).isGreaterThan(primary.getPageSize());

    // External buckets carry few but heavy records, so the slot table is sized down (256 vs 2048): saves about
    // 7.5KB of header overhead per page (file-format version EXTERNAL_BUCKET_VERSION encodes this).
    assertThat(primary.getMaxRecordsInPage()).isEqualTo(2048);
    assertThat(external.getMaxRecordsInPage()).isEqualTo(256);
  }

  @Test
  void externalBucketPathOverridePlacesFileOnSecondaryDirectory() throws java.io.IOException {
    // Persist the override on the database itself via ALTER DATABASE. This writes the value into the
    // database's configuration.json so it survives close+reopen without polluting the JVM-wide
    // GlobalConfiguration (which would leak into concurrent tests).
    final java.nio.file.Path overrideDir = java.nio.file.Files.createTempDirectory("arcadedb-ext-tier-");
    try {
      database.command("sql",
          "alter database `arcadedb.externalPropertyBucketPath` '" + overrideDir.toString() + "'");

      final DocumentType type = database.getSchema().createDocumentType("Doc");
      type.createProperty("blob", Type.STRING).setExternal(true);

      final RID[] saved = new RID[1];
      database.transaction(() -> {
        final MutableDocument d = database.newDocument("Doc").set("blob", "tiered-payload");
        d.save();
        saved[0] = d.getIdentity();
      });

      // External bucket file lives in the override directory, not the database directory. Use prefix filters
      // (not hardcoded suffixes) so the assertions stay valid if the bucket file-naming convention evolves.
      final var primary = type.getBuckets(false).getFirst();
      final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
      final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);

      // Per-database subdir is appended to the override path (so multiple DBs sharing the path can't collide).
      final java.io.File tieredDbDir = new java.io.File(overrideDir.toFile(), database.getName());
      final java.io.File[] dbDirFiles = new java.io.File(database.getDatabasePath()).listFiles(
          (dir, name) -> name.startsWith(external.getName() + "."));
      assertThat(dbDirFiles).as("external bucket should NOT be in the database directory")
          .satisfiesAnyOf(arr -> assertThat(arr).isNull(), arr -> assertThat(arr).isEmpty());

      final java.io.File[] tieredFiles = tieredDbDir.listFiles((dir, name) -> name.startsWith(external.getName() + "."));
      assertThat(tieredFiles).as("external bucket should be in <override>/<dbName>/").isNotNull().isNotEmpty();

      // Reopen: LocalDatabase.open() reloads configuration.json which contains our ALTER, so FileManager
      // rediscovers the tiered file via the secondary scan path.
      database.close();
      database = factory.open();

      final var loaded = database.lookupByRID(saved[0], true).asDocument();
      assertThat(loaded.getString("blob")).isEqualTo("tiered-payload");
    } finally {
      com.arcadedb.utility.FileUtils.deleteRecursively(overrideDir.toFile());
    }
  }

  @Test
  void checkDatabaseDetectsAndFixesOrphanedExternalRecords() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    database.transaction(() -> database.newDocument("Doc").set("blob", "referenced").save());

    final var primary = type.getBuckets(false).getFirst();
    final Integer extBucketId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket externalBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extBucketId);
    final long extCountBefore = externalBucket.count();

    // Inject an orphan: write a value blob directly to the external bucket, bypassing the property write path
    // so no primary record references it. compression="none" keeps the blob raw so cleanup just sees a normal
    // unreferenced record. The serializer write path is package-private; the test reaches it via the
    // BinarySerializerTestHelper which lives in the same package under src/test/java.
    database.transaction(() -> com.arcadedb.serializer.BinarySerializerTestHelper.injectOrphanExternalRecord(
        ((com.arcadedb.database.DatabaseInternal) database).getSerializer(),
        (com.arcadedb.database.DatabaseInternal) database, extBucketId,
        com.arcadedb.serializer.BinaryTypes.TYPE_STRING, "orphan-payload", "none"));

    assertThat(externalBucket.count()).isEqualTo(extCountBefore + 1);

    // CHECK DATABASE (no FIX): reports the orphan but does not delete it.
    final ResultSet rs = database.command("sql", "CHECK DATABASE");
    assertThat(rs.hasNext()).isTrue();
    final var row = rs.next();
    assertThat((Long) row.getProperty("orphanedExternalRecords")).isGreaterThanOrEqualTo(1L);
    assertThat((Long) row.getProperty("orphanedExternalRecordsFixed")).isEqualTo(0L);
    assertThat(externalBucket.count()).isEqualTo(extCountBefore + 1);

    // CHECK DATABASE FIX: removes the orphan.
    final ResultSet rsFix = database.command("sql", "CHECK DATABASE FIX");
    assertThat(rsFix.hasNext()).isTrue();
    assertThat((Long) rsFix.next().getProperty("orphanedExternalRecordsFixed")).isGreaterThanOrEqualTo(1L);
    assertThat(externalBucket.count()).isEqualTo(extCountBefore);

    // Re-running reports zero orphans.
    final ResultSet rsAfter = database.command("sql", "CHECK DATABASE");
    assertThat((Long) rsAfter.next().getProperty("orphanedExternalRecords")).isEqualTo(0L);
  }

  @Test
  void compressedExternalPropertyRoundTripsLargeText() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("text", Type.STRING).setExternal(true).setCompression("lz4");

    // Highly redundant text compresses to a fraction of its raw size.
    final StringBuilder sb = new StringBuilder();
    final String fragment = "the quick brown fox jumps over the lazy dog ";
    for (int i = 0; i < 200; i++)
      sb.append(fragment);
    final String value = sb.toString();
    final int rawSize = value.length();

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc").set("text", value);
      d.save();
      saved[0] = d.getIdentity();
    });

    database.close();
    database = factory.open();

    // Round-trip: the compressed bytes deserialise back to the original text.
    final var loaded = database.lookupByRID(saved[0], true).asDocument();
    assertThat(loaded.getString("text")).isEqualTo(value);

    // External bucket should hold less than half the raw text bytes (typical LZ4 ratio on repeated prose).
    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) database.getSchema().getType("Doc")).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket externalBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);
    assertThat(externalBucket.getTotalPages()).as("only one page expected for one ~9KB text record").isEqualTo(1);
    // We can't easily inspect just the record bytes, but if we shipped uncompressed the record itself would be
    // ~9KB; with LZ4 it will land far below.
  }

  @Test
  void compressionAutoSkipsWhenIncompressible() {
    final VertexType type = database.getSchema().createVertexType("V");
    type.createProperty("embedding", Type.ARRAY_OF_FLOATS).setExternal(true).setCompression("auto");

    // High-entropy float bits do not compress; auto-mode should fall back to TYPE_EXTERNAL (raw).
    final float[] embedding = new float[1024];
    final java.util.Random rnd = new java.util.Random(42);
    for (int i = 0; i < embedding.length; i++)
      embedding[i] = rnd.nextFloat() * 1000f;

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableVertex v = database.newVertex("V").set("embedding", embedding);
      v.save();
      saved[0] = v.getIdentity();
    });

    database.close();
    database = factory.open();

    // Reads back identical bytes whichever path was chosen.
    final var loaded = database.lookupByRID(saved[0], true).asVertex();
    final float[] readBack = (float[]) loaded.get("embedding");
    assertThat(readBack).isEqualTo(embedding);
  }

  @Test
  void compressionFlagPersistsAcrossReopen() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("body", Type.STRING).setExternal(true).setCompression("auto");

    assertThat(type.getProperty("body").getCompression()).isEqualToIgnoringCase("auto");

    database.close();
    database = factory.open();

    final DocumentType reloaded = database.getSchema().getType("Doc");
    assertThat(reloaded.getProperty("body").isExternal()).isTrue();
    assertThat(reloaded.getProperty("body").getCompression()).isEqualToIgnoringCase("auto");
  }

  @Test
  void sqlDdlExternalCompression() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.body STRING (EXTERNAL true, COMPRESSION 'auto')");
    });
    assertThat(database.getSchema().getType("Doc").getProperty("body").getCompression()).isEqualToIgnoringCase("auto");

    // Legacy alias: "lz4" must still be accepted and is normalised to the new "fast" tier name.
    database.transaction(() -> database.command("sql", "ALTER PROPERTY Doc.body COMPRESSION 'lz4'"));
    assertThat(database.getSchema().getType("Doc").getProperty("body").getCompression()).isEqualToIgnoringCase("fast");

    database.transaction(() -> database.command("sql", "ALTER PROPERTY Doc.body COMPRESSION 'max'"));
    assertThat(database.getSchema().getType("Doc").getProperty("body").getCompression()).isEqualToIgnoringCase("max");

    database.transaction(() -> database.command("sql", "ALTER PROPERTY Doc.body COMPRESSION 'none'"));
    assertThat(database.getSchema().getType("Doc").getProperty("body").getCompression()).isEqualToIgnoringCase("none");
  }

  @Test
  void rollbackDiscardsBothPrimaryAndExternal() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);

    final long primaryCountBefore = database.countType("Doc", false);
    final long externalCountBefore = external.count();

    database.begin();
    final MutableDocument d = database.newDocument("Doc").set("blob", "rolled-back");
    d.save();
    database.rollback();

    // Both halves of the WAL group must be reverted: primary record AND its paired external blob.
    assertThat(database.countType("Doc", false)).isEqualTo(primaryCountBefore);
    assertThat(external.count()).as("external bucket count must also be unchanged after rollback")
        .isEqualTo(externalCountBefore);
  }

  /**
   * EXTERNAL must work for top-level Type.LIST: writeExternalPropertyValue routes through serializeValue, which
   * already handles TYPE_LIST. The whole list lands in the paired bucket as one blob; reads materialise it back.
   */
  @Test
  void valueRoundTripListProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("tags", Type.LIST).setExternal(true);

    final List<Object> tags = new ArrayList<>();
    tags.add("alpha");
    tags.add(42);
    tags.add(3.14);
    tags.add("very-long-string-".repeat(50));

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc").set("name", "doc-with-list").set("tags", tags);
      d.save();
      saved[0] = d.getIdentity();
    });

    // Reopen so we exercise the deserializer, not an in-memory buffer.
    database.close();
    database = factory.open();

    final Document loaded = database.lookupByRID(saved[0], true).asDocument();
    assertThat(loaded.getString("name")).isEqualTo("doc-with-list");
    final List<Object> readBack = loaded.getList("tags");
    assertThat(readBack).containsExactlyElementsOf(tags);

    // Update the list in-place: only the external blob changes; the main record's pointer stays valid.
    final List<Object> updated = new ArrayList<>(tags);
    updated.add("appended");
    database.transaction(() -> {
      final MutableDocument m = database.lookupByRID(saved[0], true).asDocument().modify();
      m.set("tags", updated);
      m.save();
    });

    database.close();
    database = factory.open();

    final List<Object> readBackUpdated = database.lookupByRID(saved[0], true).asDocument().getList("tags");
    assertThat(readBackUpdated).containsExactlyElementsOf(updated);
  }

  /**
   * EXTERNAL must work for top-level Type.MAP. Same deal: the map serialises through TYPE_MAP into the external
   * blob. Use a LinkedHashMap so iteration order is deterministic for the assertion.
   */
  @Test
  void valueRoundTripMapProperty() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("name", Type.STRING);
    type.createProperty("attrs", Type.MAP).setExternal(true);

    final Map<String, Object> attrs = new LinkedHashMap<>();
    attrs.put("city", "Rome");
    attrs.put("zip", 100);
    attrs.put("score", 9.81);
    attrs.put("notes", "lorem ipsum ".repeat(80));

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc").set("name", "doc-with-map").set("attrs", attrs);
      d.save();
      saved[0] = d.getIdentity();
    });

    database.close();
    database = factory.open();

    final Document loaded = database.lookupByRID(saved[0], true).asDocument();
    assertThat(loaded.getString("name")).isEqualTo("doc-with-map");
    final Map<String, Object> readBack = loaded.getMap("attrs");
    // Use containsAllEntriesOf for order-insensitive equality on the materialised map.
    assertThat(readBack).containsAllEntriesOf(attrs);
    assertThat(readBack.size()).isEqualTo(attrs.size());

    // Mutate one entry; verify the external blob is rewritten in place and the change survives reopen.
    database.transaction(() -> {
      final MutableDocument m = database.lookupByRID(saved[0], true).asDocument().modify();
      final Map<String, Object> mutated = new LinkedHashMap<>(m.getMap("attrs"));
      mutated.put("zip", 200);
      m.set("attrs", mutated);
      m.save();
    });

    database.close();
    database = factory.open();

    final Map<String, Object> readBackUpdated = database.lookupByRID(saved[0], true).asDocument().getMap("attrs");
    assertThat(readBackUpdated.get("zip")).isEqualTo(200);
    assertThat(readBackUpdated.get("city")).isEqualTo("Rome");
  }

  /**
   * EXTERNAL must work for top-level Type.EMBEDDED. The embedded document's own header lives in the external blob;
   * the deserializer wires the EmbeddedModifier (parent + property) on read so the embedded knows its owner.
   */
  @Test
  void valueRoundTripEmbeddedProperty() {
    database.getSchema().createDocumentType("Address");
    final DocumentType person = database.getSchema().createDocumentType("Person");
    person.createProperty("name", Type.STRING);
    person.createProperty("address", Type.EMBEDDED).setExternal(true);

    final RID[] saved = new RID[1];
    database.transaction(() -> {
      final MutableDocument p = database.newDocument("Person").set("name", "alice");
      final MutableEmbeddedDocument addr = p.newEmbeddedDocument("Address", "address");
      addr.set("street", "Via Roma");
      addr.set("number", 7);
      addr.set("city", "Rome");
      p.save();
      saved[0] = p.getIdentity();
    });

    database.close();
    database = factory.open();

    final Document loaded = database.lookupByRID(saved[0], true).asDocument();
    assertThat(loaded.getString("name")).isEqualTo("alice");
    final EmbeddedDocument readBack = loaded.getEmbedded("address");
    assertThat(readBack).isNotNull();
    assertThat(readBack.getString("street")).isEqualTo("Via Roma");
    assertThat(readBack.getInteger("number")).isEqualTo(7);
    assertThat(readBack.getString("city")).isEqualTo("Rome");

    // Mutate the embedded by replacing it with a fresh MutableEmbeddedDocument; the rewrite hits the external bucket.
    database.transaction(() -> {
      final MutableDocument m = database.lookupByRID(saved[0], true).asDocument().modify();
      final MutableEmbeddedDocument addr = m.newEmbeddedDocument("Address", "address");
      addr.set("street", "Piazza Navona");
      addr.set("number", 99);
      addr.set("city", "Rome");
      m.save();
    });

    database.close();
    database = factory.open();

    final EmbeddedDocument readBackUpdated = database.lookupByRID(saved[0], true).asDocument().getEmbedded("address");
    assertThat(readBackUpdated.getString("street")).isEqualTo("Piazza Navona");
    assertThat(readBackUpdated.getInteger("number")).isEqualTo(99);
  }

  /**
   * On an EXTERNAL property, BOTH {@code set("field", null)} and {@code remove("field")} release the paired
   * external blob (the bucket count drops). Rationale: charging external-bucket space for a null marker is
   * surprising; users expect null to mean "no payload". The serializer special-cases null on the EXTERNAL
   * write path: it writes an inline TYPE_NULL byte in the main record's content (not a TYPE_EXTERNAL pointer)
   * and lets the orphan-cleanup pass delete any pre-existing external blob.
   * <p>
   * Read-back semantics still differ: set-null leaves the property present (returns null), remove() makes the
   * property absent (also reads as null on get(), but the property-header slot is gone).
   */
  @Test
  void externalPropertyNullAndRemoveBothReleasePairedBlob() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);

    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket external = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);

    // Two records, each with an EXTERNAL value.
    final RID[] saved = new RID[2];
    database.transaction(() -> {
      final MutableDocument a = database.newDocument("Doc").set("blob", "value-a");
      a.save();
      saved[0] = a.getIdentity();
      final MutableDocument b = database.newDocument("Doc").set("blob", "value-b");
      b.save();
      saved[1] = b.getIdentity();
    });
    assertThat(external.count()).as("two external records expected").isEqualTo(2L);

    // set-null: orphan-cleanup releases the paired bucket entry.
    database.transaction(() -> {
      final MutableDocument m = database.lookupByRID(saved[0], true).asDocument().modify();
      m.set("blob", (Object) null);
      m.save();
    });
    assertThat(external.count()).as("set-null releases the paired slot, count decreases").isEqualTo(1L);
    assertThat(database.lookupByRID(saved[0], true).asDocument().get("blob"))
        .as("set-null reads back as null").isNull();

    // remove(): drops the property entirely, orphan-cleanup releases the bucket entry.
    database.transaction(() -> {
      final MutableDocument m = database.lookupByRID(saved[1], true).asDocument().modify();
      m.remove("blob");
      m.save();
    });
    assertThat(external.count()).as("remove() releases the paired slot, count decreases to zero").isEqualTo(0L);
    assertThat(database.lookupByRID(saved[1], true).asDocument().get("blob"))
        .as("remove() makes the property absent (and absence reads as null)").isNull();

    // Reopen and confirm the on-disk state is consistent with what the in-memory bucket showed.
    database.close();
    database = factory.open();
    final var primary2 = database.getSchema().getType("Doc").getBuckets(false).getFirst();
    final Integer extId2 = ((LocalDocumentType) database.getSchema().getType("Doc")).getExternalBucketIdFor(
        primary2.getFileId());
    final LocalBucket external2 = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId2);
    assertThat(external2.count()).as("post-reopen: both slots dropped (one by set-null, one by remove)")
        .isEqualTo(0L);
    // set-null record still reads back as null after reopen; the inline TYPE_NULL byte is the canonical marker.
    assertThat(database.lookupByRID(saved[0], true).asDocument().get("blob"))
        .as("set-null record reads as null even after reopen").isNull();
  }

  /**
   * REBUILD TYPE on a database opened READ_ONLY must surface a clean DatabaseIsReadOnlyException (the
   * implicit {@code db.begin()} cannot start a write transaction). The error must wrap to a
   * CommandExecutionException that includes the read-only signal so an operator inspecting Studio sees it.
   */
  @Test
  void rebuildTypeOnReadOnlyDatabaseFailsCleanly() {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);
    database.transaction(() -> database.newDocument("Doc").set("blob", "v").save());

    database.close();
    database = factory.open(com.arcadedb.engine.ComponentFile.MODE.READ_ONLY);
    try {
      assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE Doc"))
          .isInstanceOfAny(com.arcadedb.exception.DatabaseIsReadOnlyException.class,
              com.arcadedb.exception.CommandExecutionException.class)
          .satisfies(t -> {
            final String msg = (t.getMessage() == null ? "" : t.getMessage())
                + (t.getCause() == null || t.getCause().getMessage() == null ? "" : " " + t.getCause().getMessage());
            assertThat(msg.toLowerCase(java.util.Locale.ENGLISH))
                .as("error must mention read-only state so the operator sees the actual cause")
                .containsAnyOf("read-only", "read only", "readonly");
          });
    } finally {
      // TestHelper.afterTest runs CHECK DATABASE which requires write access; reopen RW so the harness can
      // tear down cleanly without inheriting our READ_ONLY mode.
      database.close();
      database = factory.open();
    }
  }

  /**
   * Defends the DML write guard against a corrupted/old schema.json where the {@code externalBuckets} key was
   * lost. The on-disk '_ext' bucket file is still there, but the JSON map is empty. On reopen, the
   * name-based heuristic in {@code restoreExternalBuckets} must re-tag the '_ext' bucket as
   * {@code EXTERNAL_PROPERTY} so an INSERT INTO bucket:Doc_0_ext stays rejected.
   */
  @Test
  void heuristicRecoveryAdoptsOrphanExtBucketOnSchemaJsonMissingEntry() throws java.io.IOException {
    final DocumentType type = database.getSchema().createDocumentType("Doc");
    type.createProperty("blob", Type.STRING).setExternal(true);
    database.transaction(() -> database.newDocument("Doc").set("blob", "v").save());

    final var primary = type.getBuckets(false).getFirst();
    final Integer extId = ((LocalDocumentType) type).getExternalBucketIdFor(primary.getFileId());
    final LocalBucket extBucket = ((LocalSchema) database.getSchema().getEmbedded()).getBucketById(extId);
    final String extBucketName = extBucket.getName();

    database.close();

    // Strip the externalBuckets key from schema.json to simulate an older snapshot or partial corruption.
    final java.io.File schemaJson = new java.io.File(database.getDatabasePath(), "schema.json");
    String content = java.nio.file.Files.readString(schemaJson.toPath());
    final com.arcadedb.serializer.json.JSONObject schema = new com.arcadedb.serializer.json.JSONObject(content);
    final com.arcadedb.serializer.json.JSONObject docType = schema.getJSONObject("types").getJSONObject("Doc");
    docType.remove("externalBuckets");
    java.nio.file.Files.writeString(schemaJson.toPath(), schema.toString());

    database = factory.open();

    // Heuristic recovery should have re-tagged the _ext bucket; the user-bucket DML guard now rejects writes.
    // Use the Java path (more deterministic error: IllegalArgumentException with "internal" in the message).
    final MutableDocument fresh = database.newDocument("Doc").set("blob", "x");
    assertThatThrownBy(() -> database.transaction(() ->
        ((com.arcadedb.database.DatabaseInternal) database).createRecord(fresh, extBucketName)))
        .as("the heuristic must re-tag the bucket so user DML is still refused after schema.json loss")
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("internal");

    // The mapping is back in memory; record reads on the type still resolve correctly.
    final ResultSet rs = database.query("sql", "SELECT blob FROM Doc");
    assertThat(rs.hasNext()).isTrue();
    assertThat((String) rs.next().getProperty("blob")).isEqualTo("v");
  }
}
