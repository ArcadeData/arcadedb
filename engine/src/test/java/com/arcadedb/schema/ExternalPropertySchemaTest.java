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
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Schema regression tests: external property schema.
 *
 * <p>One nested class per fixed issue, each keeping its own fixture.</p>
 */
class ExternalPropertySchemaTest {
  /**
   * Regression for #9637: external bucket involved.
   *
   * Issue #9637: a type's paired {@code <bucket>_ext} bucket, which holds the values of its EXTERNAL properties, was in neither
   * {@link DocumentType#getInvolvedBuckets()} nor the per-file map {@link Schema#getInvolvedTypeByBucketId(int)} answers from.
   * That map is what the server compiles a group's per-type grants into, so an unlisted {@code _ext} file id fell through to
   * default-allow; and it is what an explicit type lock collects its files from, so the {@code _ext} bucket was never locked.
   */
  @Nested
  class ExternalBucketInvolvedTest extends TestHelper {

    @Test
    void externalBucketOfADocumentTypeResolvesToItsType() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);

      assertExternalBucketsInvolved("Doc");
    }

    @Test
    void externalBucketOfAVertexTypeResolvesToItsTypeAlongsideTheEdgeBuckets() {
      final VertexType type = database.getSchema().createVertexType("V", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);

      assertExternalBucketsInvolved("V");
      assertThat(database.getSchema().getType("V").getInvolvedBuckets().stream().map(Bucket::getName))
          .as("the edge-list buckets stay involved").contains("V_0_out_edges", "V_0_in_edges");
    }

    @Test
    void externalBucketStaysMappedAcrossReopen() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);

      reopenDatabase();

      assertExternalBucketsInvolved("Doc");
    }

    @Test
    void subtypeExternalBucketResolvesToTheSubtype() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent").withTotalBuckets(1).create();

      assertExternalBucketsInvolved("Parent");
      assertExternalBucketsInvolved("Child");
    }

    @Test
    void reclaimedExternalBucketIsNoLongerInvolved() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);
      final int extFileId = externalBucketIds((LocalDocumentType) type).getFirst();

      type.getProperty("blob").setExternal(false);
      ((LocalDocumentType) type).reclaimEmptyExternalBuckets();

      assertThat(database.getSchema().getType("Doc").getInvolvedBuckets().stream().map(Bucket::getFileId)).doesNotContain(extFileId);
      assertThat(database.getSchema().getInvolvedTypeByBucketId(extFileId)).isNull();
    }

    /** The explicit type lock collects the type's involved buckets: a write that reaches the paired bucket must be covered. */
    @Test
    void explicitTypeLockCoversTheExternalBucket() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.acquireLock().type("Doc").lock();
        rid[0] = database.newDocument("Doc").set("blob", "x".repeat(500)).save().getIdentity();
      });

      database.transaction(() -> assertThat(rid[0].asDocument().getString("blob")).isEqualTo("x".repeat(500)));
    }

    /** Every primary bucket of a multi-bucket type has its own paired bucket: the type lock must cover each of them. */
    @Test
    void explicitTypeLockCoversEveryExternalBucketOfAMultiBucketType() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 3);
      type.createProperty("blob", Type.STRING).setExternal(true);
      assertThat(externalBucketIds((LocalDocumentType) type)).hasSize(3);
      assertExternalBucketsInvolved("Doc");

      final List<Bucket> buckets = type.getBuckets(false);
      database.transaction(() -> {
        database.acquireLock().type("Doc").lock();
        for (int i = 0; i < 9; i++)
          database.newDocument("Doc").set("blob", Integer.toString(i).repeat(500)).save(buckets.get(i % 3).getName());
      });

      assertThat(database.countType("Doc", false)).isEqualTo(9);
      for (final int extId : externalBucketIds((LocalDocumentType) type))
        assertThat(database.getSchema().getBucketById(extId).count()).as("every paired bucket received values").isGreaterThan(0);
    }

    /** Same through LOCK BUCKET on the primary bucket: the write lands in the paired bucket too. */
    @Test
    void explicitBucketLockCoversTheExternalBucket() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);
      final String bucketName = type.getBuckets(false).getFirst().getName();

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.acquireLock().bucket(bucketName).lock();
        rid[0] = database.newDocument("Doc").set("blob", "y".repeat(500)).save().getIdentity();
      });

      database.transaction(() -> assertThat(rid[0].asDocument().getString("blob")).isEqualTo("y".repeat(500)));
    }

    /**
     * Locking a parent type locks its subtypes' primary buckets: their paired buckets, owned by the subtype, come with them.
     * The EXTERNAL property is declared on the subtype itself: an inherited one is written inline today (issue #9683).
     */
    @Test
    void explicitParentTypeLockCoversTheSubtypeExternalBucket() {
      database.getSchema().createDocumentType("Parent", 1);
      final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent").withTotalBuckets(1)
          .create();
      child.createProperty("blob", Type.STRING).setExternal(true);

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        database.acquireLock().type("Parent").lock();
        rid[0] = database.newDocument("Child").set("blob", "z".repeat(500)).save().getIdentity();
      });

      database.transaction(() -> {
        assertThat(rid[0].asDocument().getString("blob")).isEqualTo("z".repeat(500));
        assertThat(((DatabaseInternal) database).getSerializer().findExistingExternalRids(database, rid[0].asDocument()))
            .as("the value went to the subtype's paired bucket").containsKey("blob");
      });
    }

    private void assertExternalBucketsInvolved(final String typeName) {
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
      final List<Integer> extIds = externalBucketIds(type);
      assertThat(extIds).as("type '%s' owns a paired external bucket", typeName).isNotEmpty();

      final List<Integer> involved = type.getInvolvedBuckets().stream().map(Bucket::getFileId).toList();
      for (final int extId : extIds) {
        assertThat(involved).as("the paired external bucket is one of the type's involved buckets").contains(extId);
        assertThat(database.getSchema().getInvolvedTypeByBucketId(extId)).as("the per-file map resolves the _ext bucket to its type")
            .isSameAs(type);
        // The primary-bucket map stays primary-only: ensureExternalBucketFor relies on it to refuse adopting another type's bucket.
        assertThat(database.getSchema().getTypeByBucketId(extId)).isNull();
      }
    }

    private static List<Integer> externalBucketIds(final LocalDocumentType type) {
      return type.getBuckets(false).stream().map(b -> type.getExternalBucketIdFor(b.getFileId())).filter(id -> id != null)
          .toList();
    }
  }

  /**
   * Regression for #9683: inherited external property.
   *
   * Issue #9683: the serializer resolved an EXTERNAL property with {@link DocumentType#getPropertyIfExists(String)}, which
   * answers from the type's OWN properties only. A record of a subtype that merely inherits the EXTERNAL property therefore
   * wrote the value inline, while the subtype still owned (unused) paired {@code _ext} buckets.
   * <p>
   * The same resolution was also done against {@code record.getType()}, which a polymorphic {@code scanType} tags with the
   * QUERIED parent type even for a record living in a subtype bucket, so a subtype's own EXTERNAL property was missed on an
   * update (and its old external record leaked) and on a delete.
   */
  @Nested
  class InheritedExternalPropertyTest extends TestHelper {

    private static final String BIG  = "z".repeat(500);
    private static final String BIG2 = "w".repeat(600);

    @Test
    void newChildRecordWritesInheritedExternalValueToTheExternalBucket() {
      createParentWithExternalAndChild();

      final RID child = saveDocument("Child", BIG);

      assertExternal(child, BIG);
      assertThat(externalCount("Child")).isEqualTo(1L);
      assertThat(externalCount("Parent")).isZero();
    }

    @Test
    void grandChildRecordWritesInheritedExternalValueToTheExternalBucket() {
      createParentWithExternalAndChild();
      database.getSchema().buildDocumentType().withName("GrandChild").withSuperType("Child").withTotalBuckets(1).create();

      final RID grandChild = saveDocument("GrandChild", BIG);

      assertExternal(grandChild, BIG);
      assertThat(externalCount("GrandChild")).isEqualTo(1L);
    }

    @Test
    void childRecordWritesInheritedExternalValueAfterReopen() {
      createParentWithExternalAndChild();
      reopenDatabase();

      final RID child = saveDocument("Child", BIG);

      assertExternal(child, BIG);
      assertThat(externalCount("Child")).isEqualTo(1L);
    }

    /** A type linked to the parent after its creation (ALTER TYPE ... SUPERTYPE) gets its paired buckets then. */
    @Test
    void typeLinkedToTheParentLaterWritesInheritedExternalValueToTheExternalBucket() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().createDocumentType("Child", 1);
      database.command("sql", "ALTER TYPE Child SUPERTYPE +Parent");

      final RID child = saveDocument("Child", BIG);

      assertExternal(child, BIG);
      assertThat(externalCount("Child")).isEqualTo(1L);
    }

    @Test
    void updateOfChildRecordReusesTheExternalRecord() {
      createParentWithExternalAndChild();
      final RID child = saveDocument("Child", BIG);

      database.transaction(() -> child.asDocument().modify().set("blob", BIG2).save());

      assertExternal(child, BIG2);
      assertThat(externalCount("Child")).as("the update rewrote the same external record, nothing leaked").isEqualTo(1L);
    }

    @Test
    void deleteOfChildRecordRemovesTheInheritedExternalValue() {
      createParentWithExternalAndChild();
      final RID child = saveDocument("Child", BIG);

      database.transaction(() -> child.asDocument().delete());

      assertThat(externalCount("Child")).isZero();
    }

    @Test
    void childVertexAndChildEdgeWriteInheritedExternalValueToTheExternalBucket() {
      final VertexType parentV = database.getSchema().createVertexType("ParentV", 1);
      parentV.createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().buildVertexType().withName("ChildV").withSuperType("ParentV").withTotalBuckets(1).create();

      final EdgeType parentE = database.getSchema().createEdgeType("ParentE", 1);
      parentE.createProperty("blob", Type.STRING).setExternal(true);
      database.getSchema().buildEdgeType().withName("ChildE").withSuperType("ParentE").withTotalBuckets(1).create();

      final RID[] rids = new RID[2];
      database.transaction(() -> {
        final MutableVertex v = database.newVertex("ChildV").set("blob", BIG).save();
        rids[0] = v.getIdentity();
        rids[1] = v.newEdge("ChildE", v, "blob", BIG2).getIdentity();
      });

      assertExternal(rids[0], BIG);
      assertExternal(rids[1], BIG2);
      assertThat(externalCount("ChildV")).isEqualTo(1L);
      assertThat(externalCount("ChildE")).isEqualTo(1L);
    }

    /** The suggested remediation for records written before the fix: REBUILD TYPE on the subtype moves inline values out. */
    @Test
    void rebuildTypeOnChildMovesExistingInlineValuesToTheExternalBucket() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING);
      database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent").withTotalBuckets(1).create();

      final int n = 5;
      final List<RID> rids = new ArrayList<>();
      for (int i = 0; i < n; i++)
        rids.add(saveDocument("Child", BIG + i));

      parent.getProperty("blob").setExternal(true);

      database.transaction(() -> {
        final ResultSet rs = database.command("sql", "REBUILD TYPE Child");
        assertThat((Long) rs.next().getProperty("recordsRebuilt")).isEqualTo((long) n);
      });

      assertThat(externalCount("Child")).isEqualTo(n);
      for (int i = 0; i < n; i++)
        assertExternal(rids.get(i), BIG + i);
    }

    /**
     * A subtype record tagged with the PARENT type - what a polymorphic scan of the parent built before #11066 - used to be
     * updated and deleted with its EXTERNAL property resolved from the type that owns its bucket. Since #11206 such a record
     * is refused before anything is written, whatever the property: it would also reach the parent's listeners and skip the
     * subtype's, which no resolution of the external bucket could repair. The external value is left exactly as it was.
     */
    @Test
    void updateOfARecordTaggedWithTheParentTypeIsRefusedAndKeepsTheSubtypeExternalValue() {
      createParentAndChildWithOwnExternal();
      final RID child = saveDocument("Child", BIG);

      final Document tagged = taggedWithTheParentType(child);
      assertThat(tagged.getTypeName()).as("precondition: the record is tagged with the parent type").isEqualTo("Parent");

      assertThatThrownBy(() -> database.transaction(() -> tagged.modify().set("blob", BIG2).save()))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("'Child'");

      assertExternal(child, BIG);
      assertThat(externalCount("Child")).isEqualTo(1L);
    }

    @Test
    void deleteOfARecordTaggedWithTheParentTypeIsRefusedAndKeepsTheSubtypeExternalValue() {
      createParentAndChildWithOwnExternal();
      final RID child = saveDocument("Child", BIG);

      final Document tagged = taggedWithTheParentType(child);
      assertThat(tagged.getTypeName()).as("precondition: the record is tagged with the parent type").isEqualTo("Parent");

      assertThatThrownBy(() -> database.transaction(() -> tagged.delete()))
          .isInstanceOf(IllegalStateException.class).hasMessageContaining("'Child'");

      assertExternal(child, BIG);
      assertThat(externalCount("Child")).isEqualTo(1L);
    }

    /**
     * Between {@code setExternal(false)} and the REBUILD TYPE that inlines the values, records still point at the paired
     * bucket. Deleting one in that window must still remove its external value.
     */
    @Test
    void deleteDuringTheExternalToInlineMigrationWindowRemovesTheExternalValue() {
      final DocumentType type = database.getSchema().createDocumentType("Doc", 1);
      type.createProperty("blob", Type.STRING).setExternal(true);
      final RID rid = saveDocument("Doc", BIG);
      assertThat(externalCount("Doc")).isEqualTo(1L);

      type.getProperty("blob").setExternal(false);
      assertThat(((LocalDocumentType) type).hasExternalProperties()).isFalse();

      database.transaction(() -> rid.asDocument().delete());

      assertThat(externalCount("Doc")).isZero();
    }

    /** A read rendering (serializeForRead) of a subtype record never writes to the paired bucket, now that it is resolved. */
    @Test
    void readRenderingOfAChildRecordDoesNotTouchTheExternalBucket() {
      createParentWithExternalAndChild();
      final RID child = saveDocument("Child", BIG);

      database.transaction(() -> {
        final DatabaseInternal db = (DatabaseInternal) database;
        final MutableDocument modified = child.asDocument(true).modify().set("blob", BIG2);
        db.getSerializer().serializeForRead(db, modified);
      });

      assertThat(externalCount("Child")).isEqualTo(1L);
      assertExternal(child, BIG);
    }

    @Test
    void settingAnInheritedExternalValueToNullReleasesTheExternalRecord() {
      createParentWithExternalAndChild();
      final RID child = saveDocument("Child", BIG);
      assertThat(externalCount("Child")).isEqualTo(1L);

      database.transaction(() -> child.asDocument().modify().set("blob", null).save());

      assertThat(externalCount("Child")).isZero();
      database.transaction(() -> assertThat(child.asDocument(true).getString("blob")).isNull());
    }

    /** Every primary bucket of a multi-bucket subtype has its own paired bucket, and each one receives its records' values. */
    @Test
    void everyBucketOfAMultiBucketChildUsesItsOwnPairedBucket() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING).setExternal(true);
      final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent")
          .withTotalBuckets(3).create();

      final List<Bucket> buckets = child.getBuckets(false);
      final List<RID> rids = new ArrayList<>();
      database.transaction(() -> {
        for (int i = 0; i < 9; i++)
          rids.add(database.newDocument("Child").set("blob", BIG + i).save(buckets.get(i % 3).getName()).getIdentity());
      });

      for (int i = 0; i < 9; i++)
        assertExternal(rids.get(i), BIG + i);
      for (final Bucket primary : buckets) {
        final Integer extId = ((LocalDocumentType) child).getExternalBucketIdFor(primary.getFileId());
        assertThat(database.getSchema().getBucketById(extId).count()).as("paired bucket of %s", primary.getName()).isEqualTo(3L);
      }
    }

    /** The compression declared on the supertype's property applies to the subtype's external values too. */
    @Test
    void inheritedCompressionIsAppliedToTheSubtypeExternalValue() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING).setExternal(true).setCompression("lz4");
      database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent").withTotalBuckets(1).create();

      final String redundant = "the quick brown fox jumps over the lazy dog ".repeat(200);
      final RID child = saveDocument("Child", redundant);

      assertExternal(child, redundant);
      database.transaction(() -> {
        final RID extRid = ((DatabaseInternal) database).getSerializer().findExistingExternalRids(database, child.asDocument(true))
            .get("blob");
        final LocalBucket extBucket = (LocalBucket) database.getSchema().getBucketById(extRid.getBucketId());
        assertThat(extBucket.getRecord(extRid).size()).as("the external value is LZ4-compressed").isLessThan(redundant.length() / 2);
      });
    }

    /**
     * A type that declared its own {@code blob} before being linked to the parent keeps its own declaration: the subtype's
     * inline property shadows the parent's EXTERNAL one.
     */
    @Test
    void subtypeOwnPropertyShadowsTheInheritedExternalOne() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING).setExternal(true);
      final DocumentType child = database.getSchema().createDocumentType("Child", 1);
      child.createProperty("blob", Type.STRING);
      database.command("sql", "ALTER TYPE Child SUPERTYPE +Parent");

      final RID rid = saveDocument("Child", BIG);

      database.transaction(() -> {
        final Document doc = rid.asDocument(true);
        assertThat(((DatabaseInternal) database).getSerializer().findExistingExternalRids(database, doc)).isEmpty();
        assertThat(doc.getString("blob")).isEqualTo(BIG);
      });
      assertThat(externalCount("Child")).isZero();
    }

    /**
     * An embedded document has no bucket and so no paired external bucket: its EXTERNAL properties, declared or inherited,
     * are written inline inside the owner record instead of failing the save.
     */
    @Test
    void embeddedDocumentOfATypeWithExternalPropertiesIsWrittenInline() {
      final DocumentType address = database.getSchema().createDocumentType("Address");
      address.createProperty("street", Type.STRING).setExternal(true);
      database.getSchema().buildDocumentType().withName("ItalianAddress").withSuperType("Address").create();
      database.getSchema().createDocumentType("Person");

      final RID[] rid = new RID[1];
      database.transaction(() -> {
        final MutableDocument person = database.newDocument("Person");
        person.newEmbeddedDocument("Address", "home").set("street", BIG);
        person.newEmbeddedDocument("ItalianAddress", "work").set("street", BIG2);
        rid[0] = person.save().getIdentity();
      });

      database.transaction(() -> {
        final Document person = rid[0].asDocument(true);
        assertThat(person.getEmbedded("home").getString("street")).isEqualTo(BIG);
        assertThat(person.getEmbedded("work").getString("street")).isEqualTo(BIG2);
      });
    }

    private void createParentWithExternalAndChild() {
      final DocumentType parent = database.getSchema().createDocumentType("Parent", 1);
      parent.createProperty("blob", Type.STRING).setExternal(true);
      final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent")
          .withTotalBuckets(1).create();
      assertThat(child.getPropertyIfExists("blob")).as("precondition: the property is only inherited").isNull();
    }

    private void createParentAndChildWithOwnExternal() {
      database.getSchema().createDocumentType("Parent", 1);
      final DocumentType child = database.getSchema().buildDocumentType().withName("Child").withSuperType("Parent")
          .withTotalBuckets(1).create();
      child.createProperty("blob", Type.STRING).setExternal(true);
    }

    /**
     * A record of a subtype handed out tagged with its PARENT type. A polymorphic scan no longer does that (#11066), but any
     * caller holding a record of the wrong type must still get the subtype's own EXTERNAL property resolved from the bucket.
     */
    /** The record, built with the parent type rather than the type that owns its bucket. */
    private Document taggedWithTheParentType(final RID rid) {
      final DatabaseInternal db = (DatabaseInternal) database;
      final Binary content = ((LocalBucket) db.getSchema().getBucketById(rid.getBucketId())).getRecord(rid);
      return (Document) db.getRecordFactory().newImmutableRecord(db, db.getSchema().getType("Parent"), rid, content, null);
    }

    private RID saveDocument(final String typeName, final String blob) {
      final RID[] rid = new RID[1];
      database.transaction(() -> rid[0] = database.newDocument(typeName).set("blob", blob).save().getIdentity());
      return rid[0];
    }

    private void assertExternal(final RID rid, final String expected) {
      database.transaction(() -> {
        final Document doc = rid.asDocument(true);
        assertThat(((DatabaseInternal) database).getSerializer().findExistingExternalRids(database, doc))
            .as("the value of %s is stored in the paired external bucket", rid).containsKey("blob");
        assertThat(doc.getString("blob")).isEqualTo(expected);
      });
    }

    private long externalCount(final String typeName) {
      final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType(typeName);
      long total = 0;
      for (final Bucket primary : type.getBuckets(false)) {
        final Integer extId = type.getExternalBucketIdFor(primary.getFileId());
        if (extId != null)
          total += database.getSchema().getBucketById(extId).count();
      }
      return total;
    }
  }
}
