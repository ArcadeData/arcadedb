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
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Renaming a type re-derives the file name of every component it owns. That used to be done by guessing where
 * the component name ended: the first '_' for the bucket suffix and the first '.' for the extension tail, plus a
 * "does the new name look like a complete file name?" test of {@code contains(".") && contains("_")}. Each guess
 * is wrong for a name that itself contains the character being searched for, so a type named {@code Und_Er} was
 * renamed into a mangled file name and a type renamed to {@code na_me.dot} lost its
 * {@code .fileId.pageSize.vVersion.ext} tail entirely, which makes the reopen scan skip the file as an unknown
 * extension and silently drop the records.
 * <p>
 * The suffix is now taken from the component name the file parser already resolved, so these tests pin the
 * naming for every combination of '_' and '.' on both sides of the rename.
 */
class TypeRenameComponentNamingTest extends TestHelper {

  /** {@code <componentName>.<fileId>.<pageSize>.v<version>.<ext>} */
  private static final Pattern COMPONENT_FILE = Pattern.compile("^.+\\.\\d+\\.\\d+\\.v\\d+\\.[A-Za-z_]+$");

  @Test
  void renameTypeWhoseNameContainsUnderscore() {
    createVertexTypeWithRecords("Und_Er", 10);

    database.getSchema().getType("Und_Er").rename("Other");

    assertRenamed("Und_Er", "Other", 10);
  }

  @Test
  void renameTypeToNameContainingDot() {
    createVertexTypeWithRecords("Plain", 10);

    database.getSchema().getType("Plain").rename("ren.amed");

    assertRenamed("Plain", "ren.amed", 10);
  }

  @Test
  void renameTypeToNameContainingBothDotAndUnderscore() {
    createVertexTypeWithRecords("Src", 10);

    database.getSchema().getType("Src").rename("na_me.dot");

    assertRenamed("Src", "na_me.dot", 10);
  }

  @Test
  void renameDottedTypeToAnotherDottedName() {
    createVertexTypeWithRecords("acme.Customer", 10);

    database.getSchema().getType("acme.Customer").rename("acme.crm.Client");

    assertRenamed("acme.Customer", "acme.crm.Client", 10);
  }

  @Test
  void renameVertexTypeWithEdgesPreservesEdgeBucketMarkers() {
    database.getSchema().createVertexType("Us_er", 1);
    database.getSchema().createVertexType("Question", 1);
    database.getSchema().createEdgeType("POS_TED", 1);

    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var user = database.newVertex("Us_er").set("name", "user" + i).save();
        final var question = database.newVertex("Question").set("title", "q" + i).save();
        user.newEdge("POS_TED", question, true, new Object[0]);
      }
    });

    database.getSchema().getType("Us_er").rename("re.named");

    // The out/in edge buckets belong to the vertex type and must keep their marker suffix.
    final var edgeBuckets = database.getSchema().getType("re.named").getInvolvedBuckets().stream()
        .map(b -> b.getName()).filter(n -> n.endsWith("_out_edges") || n.endsWith("_in_edges")).toList();
    assertThat(edgeBuckets).hasSize(2);
    assertThat(edgeBuckets).allSatisfy(n -> assertThat(n).startsWith("re.named_0_"));

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.countType("re.named", true)).isEqualTo(10L);
    assertThat(database.countType("POS_TED", true)).isEqualTo(10L);
    database.transaction(() -> assertThat(database.query("sql", "select expand(out('POS_TED')) from `re.named`").stream().count())
        .isEqualTo(10L));
  }

  /**
   * A bucket attached with {@link DocumentType#addBucket} carries a name of its own (see
   * {@code Issue774AddBucketWithIndexTest}, which attaches "O202203" to type "Order"). That name is not derived
   * from the type name, so it must stay put when the type is renamed rather than be rebased onto the new name or
   * abort the rename half-way through.
   */
  @Test
  void renameTypeWithCustomNamedBucketLeavesThatBucketAlone() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Order", 1);
      type.createProperty("p1", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p1");
    });
    database.transaction(() -> database.getSchema().getType("Order")
        .addBucket(database.getSchema().createBucket("O202203")));
    database.transaction(() -> {
      database.command("sql", "INSERT INTO BUCKET:O202203 SET p1 = 'a'");
      database.command("sql", "INSERT INTO Order SET p1 = 'b'");
    });

    database.getSchema().getType("Order").rename("Orders");

    assertThat(database.getSchema().existsType("Orders")).isTrue();
    // The default bucket followed the type, the manually attached one kept its own name.
    final var bucketNames = database.getSchema().getType("Orders").getBuckets(false).stream().map(b -> b.getName()).toList();
    assertThat(bucketNames).containsExactlyInAnyOrder("Orders_0", "O202203");

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.getSchema().existsType("Orders")).isTrue();
    assertThat(database.countType("Orders", true)).isEqualTo(2L);
    database.transaction(() -> {
      assertThat(database.query("sql", "SELECT FROM Orders WHERE p1 = 'a'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "SELECT FROM Orders WHERE p1 = 'b'").stream().count()).isEqualTo(1L);
    });
  }

  /**
   * A bucket whose own name merely starts with the type name is not derived from it. Only
   * {@code <encodedType>_<something>} is, so the boundary has to be the '_' and not a bare prefix match, otherwise
   * renaming "Order" would rewrite an attached "OrderArchive" into "OrdersArchive".
   */
  @Test
  void renameTypeLeavesAloneACustomBucketSharingTheTypeNamePrefix() {
    database.transaction(() -> database.getSchema().createDocumentType("Order", 1));
    database.transaction(() -> database.getSchema().getType("Order")
        .addBucket(database.getSchema().createBucket("OrderArchive")));
    database.transaction(() -> {
      database.command("sql", "INSERT INTO BUCKET:OrderArchive SET p1 = 'a'");
      database.command("sql", "INSERT INTO Order SET p1 = 'b'");
    });

    database.getSchema().getType("Order").rename("Orders");

    final var bucketNames = database.getSchema().getType("Orders").getBuckets(false).stream().map(b -> b.getName()).toList();
    assertThat(bucketNames).containsExactlyInAnyOrder("Orders_0", "OrderArchive");

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.getSchema().existsBucket("OrderArchive")).isTrue();
    assertThat(database.countType("Orders", true)).isEqualTo(2L);
  }

  /** The HASH index rename path is separate from the LSM one and had no coverage. */
  @Test
  void renameTypeWithHashIndexKeepsIndexUsableAfterReopen() {
    database.transaction(() -> {
      database.getSchema().createDocumentType("Ha_shed", 1).createProperty("code", Type.STRING);
      database.getSchema().buildTypeIndex("Ha_shed", new String[] { "code" })
          .withType(Schema.INDEX_TYPE.HASH).withUnique(true).create();
    });
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Ha_shed").set("code", "c" + i).save();
    });

    database.getSchema().getType("Ha_shed").rename("hash.renamed");

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.countType("hash.renamed", true)).isEqualTo(10L);
    database.transaction(() -> assertThat(
        database.query("sql", "select from `hash.renamed` where code = 'c7'").stream().count()).isEqualTo(1L));
  }

  @Test
  void renameTypeWithIndexKeepsIndexUsableAfterReopen() {
    database.getSchema().createVertexType("In_Dexed", 1).createProperty("code", Type.STRING)
        .createIndex(Schema.INDEX_TYPE.LSM_TREE, true);

    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newVertex("In_Dexed").set("code", "c" + i).save();
    });

    database.getSchema().getType("In_Dexed").rename("idx.renamed");

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.countType("idx.renamed", true)).isEqualTo(10L);
    database.transaction(() -> assertThat(
        database.query("sql", "select from `idx.renamed` where code = 'c7'").stream().count()).isEqualTo(1L));
  }

  /**
   * The bucket loop rolls back what it renamed, and so must the index loop that runs after it. With two indexes, a
   * failure on the second used to leave the first one's file renamed on disk and its {@code metadata.typeName}
   * already flipped, while the type itself reverted, so the schema and the files disagreed.
   * <p>
   * The failure is injected by parking a directory on the path the last index's file is about to move to:
   * {@code Files.move} cannot replace a directory, so it raises the {@code IOException} the rename path wraps.
   */
  @Test
  void aFailedIndexRenameRollsBackTheIndexesAlreadyRenamed() throws Exception {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("Multi", 1);
      type.createProperty("p1", Type.STRING);
      type.createProperty("p2", Type.STRING);
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p1");
      type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "p2");
    });
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Multi").set("p1", "a" + i).set("p2", "b" + i).save();
    });

    final List<TypeIndex> indexes = new ArrayList<>(database.getSchema().getType("Multi").getAllIndexes(false));
    assertThat(indexes).as("two indexes are needed for a mid-loop failure").hasSize(2);

    // Block the LAST index the rename loop will reach, so at least one earlier index is renamed before the failure.
    final File databaseDirectory = new File(database.getDatabasePath());
    final String blockedComponent = indexes.getLast().getIndexesOnBuckets()[0].getName();
    final File blockedFile = fileStartingWith(databaseDirectory, blockedComponent + ".");
    final File parked = new File(databaseDirectory,
        blockedFile.getName().replace("Multi_0_", "renamed.Multi_0_"));
    assertThat(parked.mkdir()).as("fault injection: park a directory on the target path").isTrue();

    final Set<String> filesBefore = componentFileNames(databaseDirectory);

    try {
      assertThatThrownBy(() -> database.getSchema().getType("Multi").rename("renamed.Multi"))
          .isInstanceOf(SchemaException.class);

      // The type and every component it owns are back where they started.
      assertThat(database.getSchema().existsType("Multi")).isTrue();
      assertThat(database.getSchema().existsType("renamed.Multi")).isFalse();
      assertThat(componentFileNames(databaseDirectory))
          .as("every component file is back under its original name").isEqualTo(filesBefore);
    } finally {
      parked.delete();
    }

    // And the type still works, before and after a reopen.
    assertThat(database.countType("Multi", true)).isEqualTo(10L);
    database.transaction(() -> {
      assertThat(database.query("sql", "select from Multi where p1 = 'a3'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "select from Multi where p2 = 'b4'").stream().count()).isEqualTo(1L);
    });

    reopen();

    assertThat(database.countType("Multi", true)).isEqualTo(10L);
    database.transaction(() -> {
      assertThat(database.query("sql", "select from Multi where p1 = 'a3'").stream().count()).isEqualTo(1L);
      assertThat(database.query("sql", "select from Multi where p2 = 'b4'").stream().count()).isEqualTo(1L);
    });
  }

  /**
   * The forward bucket loop re-keys {@code LocalSchema.bucketMap} as each bucket is renamed. The rollback used to
   * restore only the file and the component's own name, so after a mid-loop failure the map still held the earlier
   * buckets under the name the rename was going to give them, pointing at components whose {@code getName()} had
   * gone back to the old one. Everything that resolves a bucket by name then disagrees with the schema:
   * {@code SELECT FROM BUCKET:x}, {@code existsBucket()}, the duplicate-name guard in {@code createBucket()} and the
   * keys of the statistics file.
   * <p>
   * The failure is injected the same way as {@link #aFailedIndexRenameRollsBackTheIndexesAlreadyRenamed}: a
   * directory parked on the path the last bucket's file is about to move to, which {@code Files.move} cannot
   * replace.
   */
  @Test
  void aFailedBucketRenameRestoresTheBucketMapKeys() {
    database.transaction(() -> database.getSchema().createDocumentType("Multi", 2).createProperty("p1", Type.STRING));
    database.transaction(() -> {
      for (int i = 0; i < 10; i++)
        database.newDocument("Multi").set("p1", "a" + i).save();
    });

    final List<Bucket> buckets = new ArrayList<>(database.getSchema().getType("Multi").getBuckets(false));
    assertThat(buckets).as("two buckets are needed for a mid-loop failure").hasSize(2);

    final File databaseDirectory = new File(database.getDatabasePath());
    final File parked = parkDirectoryOnRenameTarget(databaseDirectory, buckets.getLast().getName(), "Multi",
        "renamed.Multi");

    try {
      assertThatThrownBy(() -> database.getSchema().getType("Multi").rename("renamed.Multi"))
          .isInstanceOf(SchemaException.class);
    } finally {
      parked.delete();
    }

    assertBucketMapKeysMatchBucketNames();
    assertThat(database.getSchema().existsBucket("renamed.Multi_0")).as("no leftover key under the new name").isFalse();
    assertThat(database.getSchema().existsBucket("renamed.Multi_1")).as("no leftover key under the new name").isFalse();

    // The two lookups a stale key would have broken: the bucket answers to its own name again, and that name is
    // still taken, so nothing can be created over the file it already owns.
    database.transaction(() -> assertThat(database.query("sql", "select from BUCKET:Multi_0").stream().count())
        .as("the rolled-back bucket is still addressable by name").isPositive());
    assertThatThrownBy(() -> database.getSchema().createBucket("Multi_0"))
        .as("the rolled-back bucket name is still taken").isInstanceOf(SchemaException.class);

    assertThat(database.countType("Multi", true)).isEqualTo(10L);

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.countType("Multi", true)).isEqualTo(10L);
    assertBucketMapKeysMatchBucketNames();
  }

  /**
   * Same defect on the vertex-specific half of the rename: {@link LocalVertexType} renames the out/in edge buckets
   * in a second loop, with the same per-bucket re-keying and the same rollback that used to skip it.
   */
  @Test
  void aFailedEdgeBucketRenameRestoresTheBucketMapKeys() {
    database.getSchema().createVertexType("V", 1);
    database.getSchema().createVertexType("W", 1);
    database.getSchema().createEdgeType("E", 1);
    database.transaction(() -> {
      for (int i = 0; i < 10; i++) {
        final var from = database.newVertex("V").set("k", i).save();
        final var to = database.newVertex("W").set("k", i).save();
        from.newEdge("E", to, true, new Object[0]);
      }
    });

    final List<Bucket> edgeBuckets = database.getSchema().getType("V").getInvolvedBuckets().stream()
        .filter(b -> b.getName().endsWith("_out_edges") || b.getName().endsWith("_in_edges")).toList();
    assertThat(edgeBuckets).as("out and in edge buckets are needed for a mid-loop failure").hasSize(2);

    final File databaseDirectory = new File(database.getDatabasePath());
    final File parked = parkDirectoryOnRenameTarget(databaseDirectory, edgeBuckets.getLast().getName(), "V", "re.named");

    try {
      assertThatThrownBy(() -> database.getSchema().getType("V").rename("re.named"))
          .isInstanceOf(SchemaException.class);
    } finally {
      parked.delete();
    }

    assertBucketMapKeysMatchBucketNames();
    assertThat(database.getSchema().existsBucket("re.named_0_out_edges")).as("no leftover key under the new name")
        .isFalse();
    assertThat(database.getSchema().existsBucket("re.named_0")).as("no leftover key under the new name").isFalse();
    assertThat(database.getSchema().existsBucket("V_0_out_edges")).isTrue();
    assertThat(database.getSchema().existsBucket("V_0")).isTrue();

    assertThat(database.countType("V", true)).isEqualTo(10L);
    database.transaction(() -> assertThat(database.query("sql", "select expand(out('E')) from V").stream().count())
        .isEqualTo(10L));

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.countType("V", true)).isEqualTo(10L);
    assertBucketMapKeysMatchBucketNames();
    database.transaction(() -> assertThat(database.query("sql", "select expand(out('E')) from V").stream().count())
        .isEqualTo(10L));
  }

  /**
   * Fault injection: park a directory on the file path the rename of {@code componentName} is about to move to, so
   * {@code Files.move} raises an {@code IOException}. A derived component name is {@code <typeName><suffix>} and the
   * file name is that plus a {@code .fileId.pageSize.vVersion.ext} tail, both of which the rename carries over
   * untouched, so the target file name is the same with the type-name prefix swapped.
   */
  private File parkDirectoryOnRenameTarget(final File directory, final String componentName, final String oldTypeName,
      final String newTypeName) {
    assertThat(componentName).startsWith(oldTypeName + "_");
    final File blockedFile = fileStartingWith(directory, componentName + ".");
    final File parked = new File(directory, newTypeName + blockedFile.getName().substring(oldTypeName.length()));
    assertThat(parked.mkdir()).as("fault injection: park a directory on %s", parked.getName()).isTrue();
    return parked;
  }

  /**
   * Every bucket must be registered under the name it reports. A key that no longer matches is not merely untidy:
   * the bucket becomes unreachable by name while the stale key resolves to a component that answers to something
   * else.
   */
  private void assertBucketMapKeysMatchBucketNames() {
    for (final Bucket bucket : database.getSchema().getBuckets())
      assertThat(database.getSchema().getBucketByNameIfExists(bucket.getName()))
          .as("bucket '%s' must be registered under the name it reports", bucket.getName()).isSameAs(bucket);
  }

  private static File fileStartingWith(final File directory, final String prefix) {
    for (final File f : directory.listFiles())
      if (f.isFile() && f.getName().startsWith(prefix))
        return f;
    throw new AssertionError("No component file starting with '" + prefix + "' in " + directory);
  }

  /** Name -> size for every component file, so a rollback that left a file renamed shows up as a key difference. */
  /**
   * The component file NAMES, which is what a rename rollback is judged on. It used to carry each file's LENGTH as
   * well, and that made the assertion race the page flush: a bucket whose dirty page has not reached disk yet is 0
   * bytes at the "before" snapshot and one page long once anything flushes it, so an assertion about names failed
   * over a size nobody had asked about. Reproduced on CI as a lone failure in an 11801-test run.
   */
  private static Set<String> componentFileNames(final File directory) {
    final Set<String> files = new TreeSet<>();
    for (final File f : directory.listFiles())
      if (f.isFile() && !f.getName().endsWith(".json") && !f.getName().endsWith(".bin") && !f.getName().endsWith(".wal")
          && !f.getName().endsWith(".lck"))
        files.add(f.getName());
    return files;
  }

  private void createVertexTypeWithRecords(final String typeName, final int records) {
    database.getSchema().createVertexType(typeName, 1);
    database.transaction(() -> {
      for (int i = 0; i < records; i++)
        database.newVertex(typeName).set("k", i).save();
    });
  }

  private void assertRenamed(final String oldName, final String newName, final int records) {
    assertThat(database.getSchema().existsType(oldName)).isFalse();
    assertThat(database.getSchema().existsType(newName)).isTrue();
    assertThat(database.countType(newName, true)).as("records right after rename").isEqualTo((long) records);

    // The bucket keeps its index suffix and nothing else: a leaked fragment of the old name (e.g. "Other_Er_0"
    // after renaming "Und_Er") is exactly the mangling this pins.
    final var buckets = database.getSchema().getType(newName).getBuckets(false);
    for (int i = 0; i < buckets.size(); i++)
      assertThat(buckets.get(i).getName()).isEqualTo(newName + "_" + i);

    assertComponentFileNamesAreWellFormed();
    reopen();

    assertThat(database.getSchema().existsType(newName)).as("type survives reopen").isTrue();
    assertThat(database.countType(newName, true)).as("records survive reopen").isEqualTo((long) records);
  }

  /**
   * Every component file must still carry its {@code .fileId.pageSize.vVersion.ext} tail. A file that lost it is
   * not reported as an error on reopen: the directory scan just does not recognise the extension and skips it, so
   * without this check the data loss is invisible until a count comes back short.
   */
  private void assertComponentFileNamesAreWellFormed() {
    final File[] files = new File(database.getDatabasePath()).listFiles();
    assertThat(files).isNotNull();
    for (final File f : files) {
      final String name = f.getName();
      // Database-level files, not components: configuration/schema/statistics json, last-tx-id.bin, txlog wal, lock.
      if (name.endsWith(".json") || name.endsWith(".bin") || name.endsWith(".wal") || name.endsWith(".lck"))
        continue;
      assertThat(COMPONENT_FILE.matcher(name).matches())
          .as("component file '%s' lost its '.fileId.pageSize.vVersion.ext' tail", name).isTrue();
    }
  }

  private void reopen() {
    final String databasePath = database.getDatabasePath();
    database.close();
    database = new DatabaseFactory(databasePath).open();
  }
}
