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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #5780: {@code isAutomatic()} answered the opposite of the truth on a manual index.
 * <p>
 * {@code LSMTreeIndex} and {@code HashIndex} both answered {@code metadata.propertyNames != null}, and
 * {@code IndexMetadata} coerces a null property list to an EMPTY one - so the field is never null and the predicate
 * was unconditionally true, including for the one index kind for which it must be false. "Automatic" here means
 * "there are indications of what to index", which is exactly what a manual index does not have.
 * <p>
 * Two consequences, both asserted below. {@code REBUILD INDEX *} and {@code COMPACT INDEX *} select their targets on
 * that predicate, so every manual index entered the sweep and failed inside the build with the vague "metadata
 * information are missing" - a database that merely CONTAINS a manual index made the bulk repair tool report a
 * failure for something that was never rebuildable by definition. And the accurate message both statements already
 * carry ("it's manual and there aren't indications of what to index") sat behind {@code if (!idx.isAutomatic())},
 * which could never be entered.
 */
public class Issue5780ManualIndexIsAutomaticTest extends TestHelper {
  private static final String TYPE_NAME   = "Doc";
  private static final String MANUAL_LSM  = "manualLsmIdx";
  private static final String MANUAL_HASH = "manualHashIdx";

  /**
   * Offset of the first key-type byte on a hash index metadata page, mirroring the package-private
   * {@code HashIndexBucket.META_KEY_TYPES_START} this package cannot see.
   */
  private static final int HASH_INDEX_META_KEY_TYPES_START = 9;

  private RID    RID_1;
  private RID    RID_2;
  private String typeIndexName;

  @Override
  public void beginTest() {
    database.transaction(() -> {
      database.getSchema().createDocumentType(TYPE_NAME).createProperty("name", Type.STRING);
      typeIndexName = database.getSchema().buildTypeIndex(TYPE_NAME, new String[] { "name" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create().getName();

      RID_1 = database.newDocument(TYPE_NAME).set("name", "one").save().getIdentity();
      RID_2 = database.newDocument(TYPE_NAME).set("name", "two").save().getIdentity();
    });
  }

  /**
   * The predicate itself. A manual index of either kind that can be built without a type answers false, and a type
   * index - the wrapper and its bucket sub-index alike - keeps answering true.
   */
  @Test
  void aManualIndexIsNotAutomaticAndATypeIndexStillIs() {
    final Index manualLsm = createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);
    final Index manualHash = createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

    assertThat(manualLsm.isAutomatic())
        .as("an LSM_TREE manual index has no indications of what to index")
        .isFalse();
    assertThat(manualHash.isAutomatic())
        .as("a HASH manual index has no indications of what to index")
        .isFalse();

    final Index typeIndex = database.getSchema().getIndexByName(typeIndexName);
    assertThat(typeIndex.isAutomatic()).isTrue();
    for (final Index bucketIndex : ((TypeIndex) typeIndex).getIndexesOnBuckets())
      assertThat(bucketIndex.isAutomatic())
          .as("bucket sub-index '%s' is derived from the type's records", bucketIndex.getName())
          .isTrue();
  }

  /**
   * {@code REBUILD INDEX *} no longer targets what it cannot rebuild: the manual indexes are absent from the rebuilt
   * list AND - the assertion that discriminates the fix from the bug - absent from {@code failedIndexes}, which is
   * where they used to land. The automatic index is still rebuilt, so the sweep did not simply stop doing its job.
   */
  @Test
  void rebuildIndexAllSkipsManualIndexesInsteadOfFailingOnThem() {
    final Index manualLsm = createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);
    final Index manualHash = createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

    database.transaction(() -> {
      manualLsm.put(new Object[] { "a" }, new RID[] { RID_1 });
      manualHash.put(new Object[] { "b" }, new RID[] { RID_2 });
    });

    final Result row = database.command("sql", "rebuild index *").next();

    final List<String> rebuilt = row.getProperty("indexes");
    assertThat(rebuilt).doesNotContain(MANUAL_LSM, MANUAL_HASH);
    assertThat(rebuilt)
        .as("the sweep must still rebuild the automatic bucket sub-indexes")
        .isNotEmpty();

    assertThat((Object) row.getProperty("failedIndexes"))
        .as("a manual index is not a broken index: it must not be reported as a rebuild failure")
        .isNull();

    // The entries are still there. build() raised before emptying the index even before the fix, so this pins that
    // the fix did not introduce a destructive path of its own.
    database.transaction(() -> {
      assertThat(manualLsm.get(new Object[] { "a" }).next()).isEqualTo(RID_1);
      assertThat(manualHash.get(new Object[] { "b" }).next()).isEqualTo(RID_2);
    });
  }

  /**
   * A named rebuild of a manual index reports what is actually wrong with the request, from the guard written for
   * exactly this case, instead of the "metadata information are missing" raised two layers down inside the build.
   */
  @Test
  void rebuildIndexNamedOnAManualIndexReportsThatItIsManual() {
    createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

    assertThatThrownBy(() -> database.command("sql", "rebuild index `" + MANUAL_LSM + "`"))
        .isInstanceOf(IndexException.class)
        .hasMessageContaining(MANUAL_LSM)
        .hasMessageContaining("it's manual and there aren't indications of what to index");
  }

  /**
   * {@code COMPACT INDEX} selects its targets on the same predicate and carries the same guard, so the fix reaches
   * it too: the sweep skips a manual index and a named request is refused by the message that explains why.
   */
  @Test
  void compactIndexFollowsTheSamePredicate() {
    final Index manualLsm = createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

    database.transaction(() -> manualLsm.put(new Object[] { "a" }, new RID[] { RID_1 }));

    final Result row = database.command("sql", "compact index *").next();
    assertThat((List<String>) row.getProperty("indexes")).doesNotContain(MANUAL_LSM);

    assertThatThrownBy(() -> database.command("sql", "compact index `" + MANUAL_LSM + "`"))
        .isInstanceOf(IndexException.class)
        .hasMessageContaining(MANUAL_LSM)
        .hasMessageContaining("it's manual and there aren't indications of what to index");

    // The sweep left the entries alone.
    database.transaction(() -> assertThat(manualLsm.get(new Object[] { "a" }).next()).isEqualTo(RID_1));
  }

  /**
   * The other side of the predicate flip: a NAMED rebuild and a NAMED compaction of an automatic index still run.
   * <p>
   * The sweep test above already pins that the {@code *} form keeps rebuilding the bucket sub-indexes, but both
   * statements guard their single-index path on the same predicate, and that path takes an index the sweep never
   * hands it - the {@code TypeIndex} wrapper, which {@code *} deliberately excludes.
   */
  @Test
  void namedRebuildAndCompactStillRunOnAnAutomaticIndex() {
    // Present in the same database, so the flip is exercised with both kinds side by side.
    createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

    final Result rebuilt = database.command("sql", "rebuild index `" + typeIndexName + "`").next();
    assertThat((List<String>) rebuilt.getProperty("indexes")).contains(typeIndexName);
    assertThat((Object) rebuilt.getProperty("failedIndexes")).isNull();

    final String bucketIndexName = ((TypeIndex) database.getSchema().getIndexByName(typeIndexName))
        .getIndexesOnBuckets()[0].getName();
    final Result compacted = database.command("sql", "compact index `" + bucketIndexName + "`").next();
    assertThat((List<String>) compacted.getProperty("indexes")).contains(bucketIndexName);

    // The index still resolves its records after both operations.
    database.transaction(() -> assertThat(database.getSchema().getIndexByName(typeIndexName)
        .get(new Object[] { "one" }).next()).isEqualTo(RID_1));
  }

  /**
   * The predicate is public API and is reported verbatim by the schema views, so an operator listing the indexes now
   * reads the truth about which ones something populates.
   */
  @Test
  void theSchemaViewReportsAManualIndexAsNotAutomatic() {
    createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);

    boolean seenManual = false;
    boolean seenAutomatic = false;
    final ResultSet rs = database.query("sql", "select from schema:indexes");
    while (rs.hasNext()) {
      final Result row = rs.next();
      final String name = row.getProperty("name");
      if (MANUAL_LSM.equals(name)) {
        assertThat((Boolean) row.getProperty("automatic")).isFalse();
        seenManual = true;
      } else {
        assertThat((Boolean) row.getProperty("automatic"))
            .as("index '%s' is bound to a type, so it stays automatic", name)
            .isTrue();
        seenAutomatic = true;
      }
    }

    assertThat(seenManual).as("schema:indexes must list the manual index").isTrue();
    assertThat(seenAutomatic).as("schema:indexes must list the type indexes too").isTrue();
  }

  /**
   * A manual index whose name carries no underscore does not abort the schema load.
   * <p>
   * The orphan-relinking pass walks every TYPE-LESS index and splits its name on the last underscore to recover the
   * bucket a renamed sub-index belongs to - those are always {@code <bucketName>_<timestamp>}. A manual index is
   * type-less too but is named by the caller, so {@code lastIndexOf} answered -1 and the substring raised
   * {@code StringIndexOutOfBoundsException}, which {@code readConfiguration}'s own catch reported as "Error on
   * loading schema. The schema will be reset".
   * <p>
   * What that actually costs is everything the loader had not reached yet, since the pass sits in the middle of
   * {@code readConfiguration}: the bucket selection strategies, the triggers, the materialized views and continuous
   * aggregates, the function libraries, the extensions, and the compaction file-migration map WAL recovery redirects
   * through. The bucket selection strategy is the cheapest of those to pin, and it is the FIRST one after the pass,
   * so it is what this test asserts - the types themselves are parsed BEFORE the pass and survive either way, which
   * is why asserting their presence alone would not have caught this.
   */
  @Test
  void aManualIndexWithNoUnderscoreInItsNameDoesNotAbortTheSchemaLoad() {
    database.getSchema().getType(TYPE_NAME).setBucketSelectionStrategy("thread");

    createManualIndex(MANUAL_LSM, Schema.INDEX_TYPE.LSM_TREE, false);
    createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

    reopenDatabase();

    assertThat(database.getSchema().getType(TYPE_NAME).getBucketSelectionStrategy().getName())
        .as("the load must reach the settings that follow the orphan-relinking pass")
        .isEqualTo("thread");

    assertThat(database.getSchema().existsType(TYPE_NAME)).isTrue();
    assertThat(database.getSchema().existsIndex(typeIndexName)).isTrue();
    assertThat(database.getSchema().existsIndex(MANUAL_LSM)).isTrue();
    assertThat(database.getSchema().existsIndex(MANUAL_HASH)).isTrue();
    assertThat(database.countType(TYPE_NAME, false)).isEqualTo(2L);

    assertThat(database.getSchema().getIndexByName(MANUAL_LSM).isAutomatic()).isFalse();
    assertThat(database.getSchema().getIndexByName(MANUAL_HASH).isAutomatic()).isFalse();
  }

  /**
   * {@code CHECK DATABASE FIX} reports a corrupt manual index and carries on, instead of aborting the whole repair on
   * it.
   * <p>
   * The rebuild loop resolves the index's associated bucket to recreate it there. A manual index has none, so
   * {@code getBucketById(-1)} raised "Bucket with id '-1' was not found" and the exception escaped the FIX before ANY
   * index - including the healthy type indexes of the same database - had been repaired. The index is also left
   * alone rather than dropped and recreated: its entries are not derived from any record, so the recreation this
   * loop performs would destroy them.
   */
  @Test
  void checkDatabaseFixReportsAManualIndexInsteadOfAbortingOnIt() {
    createManualIndex(MANUAL_HASH, Schema.INDEX_TYPE.HASH, true);

    // The exact corruption HashIndexMetadataCorruptionTest injects: an invalid key-type byte on the metadata page.
    // It has to be re-read from disk to reach the cached key types, hence the reopen.
    corruptHashIndexKeyType(MANUAL_HASH);
    reopenDatabase();

    final Result row = database.command("sql", "check database fix").next();

    assertThat((Collection<String>) row.getProperty("corruptedIndexes"))
        .as("the finding must still reach the operator")
        .contains(MANUAL_HASH);
    assertThat((Collection<String>) row.getProperty("rebuiltIndexes"))
        .as("a manual index cannot be rebuilt from records, so FIX must not claim it rebuilt one")
        .doesNotContain(MANUAL_HASH);
    assertThat((Collection<String>) row.getProperty("warnings"))
        .anyMatch(w -> w.contains(MANUAL_HASH) && w.contains("manual index"));

    // The index is still there: FIX did not drop it, which on a manual index is an unrecoverable delete of its entries.
    assertThat(database.getSchema().existsIndex(MANUAL_HASH)).isTrue();
    // ...and the rest of the schema came through the repair pass untouched.
    assertThat(database.getSchema().existsIndex(typeIndexName)).isTrue();
    assertThat(database.countType(TYPE_NAME, false)).isEqualTo(2L);

    // Apply the repair the warning prescribes, so the class-wide post-test integrity check sees a clean database
    // rather than the corruption this test deliberately injected.
    database.getSchema().dropIndex(MANUAL_HASH);
  }

  /**
   * Overwrites the first key-type byte of a hash index metadata page (page 0) with a value no key type uses, which is
   * what {@code checkMetadataIntegrity()} reports as corruption.
   */
  private void corruptHashIndexKeyType(final String indexName) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = ((IndexInternal) db.getSchema().getIndexByName(indexName)).getFileIds().get(0);
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, 0), pageSize, false);
        page.writeByte(HASH_INDEX_META_KEY_TYPES_START, (byte) -108);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private Index createManualIndex(final String name, final Schema.INDEX_TYPE indexType, final boolean unique) {
    return database.getSchema().buildManualIndex(name, new Type[] { Type.STRING })
        .withType(indexType).withUnique(unique).create();
  }
}
