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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression test for issue #10247: every schema change serialized every type again and rebuilt the bucket-to-type maps
 * over every type, so a DDL statement cost O(N) in the number of types and creating N types O(N^2); opening the database
 * compared every bucket with every type. A change made through a type now serializes that type's hierarchy only, the other
 * types are written from the JSON they kept, the maps follow the buckets attached and detached, and the open looks the
 * owner of a bucket up.
 * <p>
 * The test JVM runs with {@code arcadedb.schema.verifyIncrementalSerialization}: every save compares what it writes with the
 * full serialization and every save that skips the rebuild of the maps compares them with one, and fails on a difference.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10247IncrementalSchemaSaveTest extends TestHelper {

  @Test
  void verificationIsOnInTheTestSuite() {
    // WITHOUT IT, A CHANGE THAT REACHES A TYPE'S JSON WITHOUT INVALIDATING IT WOULD GO UNNOTICED BY THE WHOLE SUITE
    assertThat(LocalSchema.VERIFY_INCREMENTAL_SERIALIZATION).isTrue();
  }

  @Test
  void aChangeToOneTypeDoesNotSerializeTheOthersAgain() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    for (int i = 0; i < 20; i++)
      schema.createDocumentType("T" + i).createProperty("p", Type.STRING);

    final LocalDocumentType untouched = schema.getType("T3");
    final String untouchedJSON = untouched.getSerializedJSON();
    final String changedJSONBefore = schema.getType("T7").getSerializedJSON();

    schema.getType("T7").createProperty("added", Type.INTEGER);

    // THE SAME INSTANCE: THE SAVE REUSED IT RATHER THAN SERIALIZING THE TYPE AGAIN
    assertThat(untouched.getSerializedJSON()).isSameAs(untouchedJSON);
    assertThat(schema.getType("T7").getSerializedJSON()).isNotSameAs(changedJSONBefore).contains("\"added\"");
    assertFileIsTheFullSerialization();
  }

  @Test
  void aNewTypeDoesNotSerializeTheOthersAgain() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("A").createProperty("p", Type.STRING);
    final String json = schema.getType("A").getSerializedJSON();

    schema.createDocumentType("B");
    schema.createVertexType("V1");
    schema.createEdgeType("E1");

    assertThat(schema.getType("A").getSerializedJSON()).isSameAs(json);
    assertFileIsTheFullSerialization();
  }

  @Test
  void aChangeToASuperTypeReachesTheHierarchy() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("Parent").createProperty("id", Type.LONG);
    schema.buildDocumentType().withName("Child").withSuperType("Parent").create();
    schema.buildDocumentType().withName("GrandChild").withSuperType("Child").create();
    schema.createDocumentType("Unrelated");

    final String unrelated = schema.getType("Unrelated").getSerializedJSON();
    final String grandChild = schema.getType("GrandChild").getSerializedJSON();

    // AN INDEX ON THE ROOT TYPE GETS A SUB-INDEX ON EVERY BUCKET OF THE SUB TYPES
    schema.getType("Parent").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "id");

    assertThat(schema.getType("Unrelated").getSerializedJSON()).isSameAs(unrelated);
    assertThat(schema.getType("GrandChild").getSerializedJSON()).isNotSameAs(grandChild);
    assertFileIsTheFullSerialization();

    // UNLINKED: BOTH SIDES OF THE CHANGE ARE WRITTEN AGAIN
    schema.getType("GrandChild").removeSuperType("Child");
    assertFileIsTheFullSerialization();

    reopenDatabase();
    assertThat(database.getSchema().getType("GrandChild").getSuperTypes()).isEmpty();
    assertThat(database.getSchema().getType("Parent").getAllIndexes(false)).hasSize(1);
  }

  @Test
  void changesThatNameNoTypeWriteEveryTypeAfresh() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("Doc").createProperty("name", Type.STRING);
    schema.getType("Doc").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "name");
    schema.createDocumentType("Other");

    // A PROPERTY ATTRIBUTE IS SAVED WITHOUT NAMING ITS TYPE: THE SAVE SERIALIZES THEM ALL
    schema.getType("Doc").getProperty("name").setMandatory(true);
    assertFileIsTheFullSerialization();

    // A RENAME CHANGES NAMES OTHER TYPES CAN REFER TO
    schema.getType("Other").rename("Renamed");
    assertFileIsTheFullSerialization();

    // AN INDEX COMPACTION SWAPS THE FILE A TYPE SERIALIZES
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++)
        database.newDocument("Doc").set("name", "n" + i).save();
    });
    final TypeIndex index = (TypeIndex) schema.getType("Doc").getPolymorphicIndexByProperties("name");
    for (final var bucketIndex : index.getIndexesOnBuckets()) {
      bucketIndex.scheduleCompaction();
      bucketIndex.compact();
    }
    schema.getType("Renamed").createProperty("late", Type.STRING);
    assertFileIsTheFullSerialization();

    reopenDatabase();
    assertThat(database.query("sql", "SELECT count(*) AS c FROM Doc WHERE name = 'n7'").next().<Long>getProperty("c")).isEqualTo(1L);
    assertThat(database.getSchema().existsType("Renamed")).isTrue();
    assertThat(database.getSchema().getType("Renamed").existsProperty("late")).isTrue();
  }

  @Test
  void renameAndDropOfATypeWithSubTypesAreWrittenAsTheFullSerialization() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createVertexType("Animal").createProperty("name", Type.STRING);
    schema.buildVertexType().withName("Dog").withSuperType("Animal").create();
    schema.buildVertexType().withName("Puppy").withSuperType("Dog").create();
    schema.createDocumentType("Owner").createProperty("pet", Type.LINK).setOfType("Dog");

    // THE SUB TYPES NAME THE RENAMED TYPE AS THEIR PARENT, AND ANOTHER TYPE LINKS TO IT
    schema.getType("Dog").rename("Hound");
    assertFileIsTheFullSerialization();

    schema.getType("Owner").createProperty("since", Type.DATETIME);
    assertFileIsTheFullSerialization();

    // THE DROP UNLINKS THE SUB TYPE FROM IT
    schema.getType("Puppy").removeSuperType("Hound");
    schema.dropType("Hound");
    schema.getType("Puppy").createProperty("age", Type.INTEGER);
    assertFileIsTheFullSerialization();

    reopenDatabase();
    assertThat(database.getSchema().existsType("Hound")).isFalse();
    assertThat(database.getSchema().getType("Puppy").existsProperty("age")).isTrue();
  }

  @Test
  void aSchemaWrittenFromOutsideIsSerializedAfreshByTheNextSave() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("A").createProperty("p", Type.STRING);
    schema.createDocumentType("B");

    // WHAT AN HA FOLLOWER DOES WITH THE LEADER'S SCHEMA: WRITE IT, THEN LOAD IT
    final JSONObject replicated = schema.toJSON();
    replicated.getJSONObject("types").getJSONObject("A").put("custom", new JSONObject().put("fromLeader", "yes"));
    schema.update(replicated);
    schema.load(ComponentFile.MODE.READ_WRITE, true);
    assertThat(schema.getType("A").getCustomValue("fromLeader")).isEqualTo("yes");

    // A CHANGE TO ANOTHER TYPE: THE FILE STILL CARRIES WHAT THE LEADER WROTE FOR A
    schema.getType("B").createProperty("q", Type.STRING);
    assertFileIsTheFullSerialization();
    assertThat(Files.readString(schema.getConfigurationFile().toPath(), StandardCharsets.UTF_8)).contains("fromLeader");
  }

  @Test
  void theSwitchTurnsTheCacheOff() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("A");
    schema.createDocumentType("B");
    final String json = schema.getType("A").getSerializedJSON();

    database.getConfiguration().setValue(GlobalConfiguration.SCHEMA_INCREMENTAL_SERIALIZATION, false);
    try {
      schema.getType("B").createProperty("p", Type.STRING);
      // EVERY TYPE SERIALIZED AGAIN, AS BEFORE THE CACHE EXISTED
      assertThat(schema.getType("A").getSerializedJSON()).isNotSameAs(json).isEqualTo(json);
      assertFileIsTheFullSerialization();
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.SCHEMA_INCREMENTAL_SERIALIZATION, true);
    }
  }

  @Test
  void concurrentSchemaChangesAreWrittenAsTheFullSerialization() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("Base").createProperty("id", Type.LONG);
    schema.getType("Base").createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");

    // EVERY SAVE OF EVERY THREAD IS VERIFIED AGAINST THE FULL SERIALIZATION: A DIFFERENCE FAILS THE DDL STATEMENT
    final int threads = 4;
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final List<Thread> workers = new ArrayList<>();
    for (int t = 0; t < threads; t++) {
      final int thread = t;
      workers.add(new Thread(() -> {
        try {
          for (int i = 0; i < 15; i++) {
            final String name = "C" + thread + "_" + i;
            final DocumentType type = schema.buildDocumentType().withName(name).withSuperType("Base").create();
            type.createProperty("p", Type.STRING);
            if (i % 3 == 0)
              type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p");
            if (i % 5 == 0)
              type.setCustomValue("k", "v" + i);
            if (i % 7 == 0)
              schema.dropType(name);
          }
        } catch (final Throwable e) {
          failure.compareAndSet(null, e);
        }
      }, "issue10247-ddl-" + t));
    }
    workers.forEach(Thread::start);
    for (final Thread worker : workers)
      worker.join();

    assertThat(failure.get()).isNull();
    assertFileIsTheFullSerialization();

    final String before = schema.toJSON().toString();
    reopenDatabase();
    // THE SCHEMA READ BACK IS THE ONE THAT WAS WRITTEN, VERSION APART
    assertThat(((LocalSchema) database.getSchema()).toJSON().getJSONObject("types").toString()).isEqualTo(
        new JSONObject(before).getJSONObject("types").toString());
  }

  @Test
  void aliasesCustomValuesAndEscapedNamesAreWrittenAsTheFullSerialization() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    // AN ALIAS SORTS BEFORE ITS TYPE'S NAME: THE TYPE IS WRITTEN AT ITS FIRST NAME, AS THE FULL SERIALIZATION DOES
    schema.createDocumentType("Zeta").setAliases(Set.of("Alpha", "Mid"));
    schema.createDocumentType("Città_é中").createProperty("na\"me", Type.STRING);
    schema.createDocumentType("Beta");
    schema.getType("Zeta").setCustomValue("quote", "say \"hi\" \\ \n");
    schema.getType("Zeta").createProperty("p", Type.STRING).setCustomValue("unicode", "è€");
    assertFileIsTheFullSerialization();

    // THE ORDER IS COMPUTED AGAIN WHEN THE NAMES CHANGE
    schema.getType("Zeta").setAliases(Set.of("Omega"));
    schema.createDocumentType("Aardvark");
    assertFileIsTheFullSerialization();

    reopenDatabase();
    assertThat(database.getSchema().getType("Omega").getName()).isEqualTo("Zeta");
    assertThat(database.getSchema().getType("Città_é中").existsProperty("na\"me")).isTrue();
    assertThat(database.getSchema().getType("Zeta").getCustomValue("quote")).isEqualTo("say \"hi\" \\ \n");
  }

  @Test
  void ddlInsideATransactionIsWrittenAtItsEnd() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.createDocumentType("Before");

    database.transaction(() -> {
      schema.createDocumentType("Inside").createProperty("x", Type.STRING);
      final MutableDocument doc = database.newDocument("Inside");
      doc.set("x", "v").save();
    });
    assertFileIsTheFullSerialization();

    reopenDatabase();
    assertThat(database.getSchema().getType("Inside").existsProperty("x")).isTrue();
    assertThat(database.countType("Inside", false)).isEqualTo(1);
  }

  @Test
  void bucketMapsFollowAttachDetachAndDrop() {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final DocumentType a = schema.buildDocumentType().withName("A").withTotalBuckets(2).create();
    final DocumentType b = schema.buildDocumentType().withName("B").withTotalBuckets(1).create();

    for (final Bucket bucket : a.getBuckets(false))
      assertThat(schema.getTypeByBucketId(bucket.getFileId())).isSameAs(a);

    // A BUCKET BELONGS TO ONE TYPE: THE OWNER IS FOUND BY LOOKUP, NO LONGER BY ASKING EVERY TYPE
    final Bucket first = a.getBuckets(false).getFirst();
    assertThatThrownBy(() -> b.addBucket(first)).isInstanceOf(SchemaException.class)
        .hasMessageContaining("already associated to the type 'A'");

    a.removeBucket(first);
    assertThat(schema.getTypeByBucketId(first.getFileId())).isNull();
    b.addBucket(first);
    assertThat(schema.getTypeByBucketId(first.getFileId())).isSameAs(b);
    assertThat(schema.getInvolvedTypeByBucketId(first.getFileId())).isSameAs(b);

    final int bucketOfB = b.getBuckets(false).getFirst().getFileId();
    schema.dropType("B");
    assertThat(schema.getTypeByBucketId(bucketOfB)).isNull();
    assertThat(schema.getTypeByBucketId(-1)).isNull();
    assertThat(schema.getTypeByBucketId(Integer.MAX_VALUE)).isNull();
  }

  @Test
  void aBucketOwnedTwiceInTheSchemaFileIsStillRefusedAtOpen() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    schema.buildDocumentType().withName("Owner").withTotalBuckets(1).create();
    schema.buildDocumentType().withName("Thief").withTotalBuckets(1).create();
    final String ownerBucket = schema.getType("Owner").getBuckets(false).getFirst().getName();
    final String thiefBucket = schema.getType("Thief").getBuckets(false).getFirst().getName();
    database.close();

    // HAND-EDITED SCHEMA: THE SECOND TYPE CLAIMS THE FIRST TYPE'S BUCKET
    final File schemaFile = new File(getDatabasePath(), LocalSchema.SCHEMA_FILE_NAME);
    final String content = Files.readString(schemaFile.toPath(), StandardCharsets.UTF_8);
    Files.writeString(schemaFile.toPath(), content.replace("\"" + thiefBucket + "\"", "\"" + ownerBucket + "\""),
        StandardCharsets.UTF_8);

    database = factory.open();
    // THE LOAD REFUSES THE SECOND CLAIM, AS IT DID WHEN IT ASKED EVERY TYPE: THE OWNER KEEPS ITS BUCKET
    assertThat(database.getSchema().getTypeByBucketName(ownerBucket).getName()).isEqualTo("Owner");
  }

  private void assertFileIsTheFullSerialization() throws Exception {
    final LocalSchema schema = (LocalSchema) database.getSchema();
    final String onDisk = Files.readString(schema.getConfigurationFile().toPath(), StandardCharsets.UTF_8);
    assertThat(onDisk).isEqualTo(schema.toJSON().toString());
  }
}
