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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9683: the serializer resolved an EXTERNAL property with {@link DocumentType#getPropertyIfExists(String)}, which
 * answers from the type's OWN properties only. A record of a subtype that merely inherits the EXTERNAL property therefore
 * wrote the value inline, while the subtype still owned (unused) paired {@code _ext} buckets.
 * <p>
 * The same resolution was also done against {@code record.getType()}, which a polymorphic {@code scanType} tags with the
 * QUERIED parent type even for a record living in a subtype bucket, so a subtype's own EXTERNAL property was missed on an
 * update (and its old external record leaked) and on a delete.
 */
class Issue9683InheritedExternalPropertyTest extends TestHelper {

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
   * A polymorphic scan of the parent tags a subtype record with the PARENT type. The subtype's own EXTERNAL property must
   * still be resolved from the type that owns the record's bucket: the update must stay external and reuse the old record.
   */
  @Test
  void updateOfARecordTaggedWithTheParentTypeKeepsTheSubtypeExternalProperty() {
    createParentAndChildWithOwnExternal();
    final RID child = saveDocument("Child", BIG);

    final Document scanned = scanParentPolymorphically();
    assertThat(scanned.getTypeName()).as("precondition: the scan tags the record with the parent type").isEqualTo("Parent");

    database.transaction(() -> scanned.modify().set("blob", BIG2).save());

    assertExternal(child, BIG2);
    assertThat(externalCount("Child")).as("the old external record was reused, not leaked").isEqualTo(1L);
  }

  @Test
  void deleteOfARecordTaggedWithTheParentTypeRemovesTheSubtypeExternalValue() {
    createParentAndChildWithOwnExternal();
    saveDocument("Child", BIG);

    final Document scanned = scanParentPolymorphically();
    assertThat(scanned.getTypeName()).as("precondition: the scan tags the record with the parent type").isEqualTo("Parent");

    database.transaction(() -> scanned.delete());

    assertThat(externalCount("Child")).isZero();
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

  private Document scanParentPolymorphically() {
    final Document[] found = new Document[1];
    database.transaction(() -> database.scanType("Parent", true, record -> {
      found[0] = record;
      return false;
    }));
    assertThat(found[0]).isNotNull();
    return found[0];
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
