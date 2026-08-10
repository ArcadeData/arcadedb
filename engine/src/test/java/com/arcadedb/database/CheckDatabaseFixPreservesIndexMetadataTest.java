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

import com.arcadedb.TestHelper;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.FullTextIndexMetadata;
import com.arcadedb.schema.GeoIndexMetadata;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5934: {@code CHECK DATABASE FIX} drops and rebuilds a bucket sub-index whose bucket held a
 * corrupted record, through the same {@code dropIndex} + rebuild sequence {@code RebuildIndexStatement} used before
 * issues #5791/#4732 were fixed for it (see {@code RebuildIndexPreservesNameTest}) - but
 * {@link com.arcadedb.engine.DatabaseChecker}'s auto-fix path never received the equivalent fix: it called
 * {@code buildBucketIndex(...)} with no {@code withIndexName(...)} and no {@code withMetadata(...)} at all. On a
 * single-bucket type (the common default) dropping the only sub-index also drops the owning {@link TypeIndex}
 * wrapper, so an explicitly-named index silently reverted to the auto-derived {@code typeName[properties]} form, and
 * a {@code FULL_TEXT}/{@code GEOSPATIAL} index lost its analyzer/precision settings, every time {@code CHECK DATABASE
 * FIX} repaired one of their bucket sub-indexes.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseFixPreservesIndexMetadataTest extends TestHelper {

  @Test
  void fixPreservesNameAndAnalyzerOnFullTextIndex() {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      final Result inserted = database.command("sql", "INSERT INTO Doc SET title = 'java database tutorial'").next();
      database.command("sql", "CREATE INDEX ftDocTitle ON Doc (title) FULL_TEXT METADATA "
          + "{\"analyzer\": \"org.apache.lucene.analysis.core.KeywordAnalyzer\"}");
      victim.set(inserted.toElement().getIdentity());
    });

    corruptRecordTypeByte(victim.get());

    final ResultSet result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("autoFix") > 0L).isTrue();

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("ftDocTitle")).isTrue();
      assertThat(database.getSchema().existsIndex("Doc[title]")).isFalse();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("ftDocTitle");
      assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.FULL_TEXT);
      final FullTextIndexMetadata metadata = (FullTextIndexMetadata) idx.getMetadataForNewFile();
      assertThat(metadata.getAnalyzerClass()).isEqualTo("org.apache.lucene.analysis.core.KeywordAnalyzer");
    });
  }

  @Test
  void fixPreservesNameAndPrecisionOnGeospatialIndex() {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Place");
      database.command("sql", "CREATE PROPERTY Place.location STRING");
      final Result inserted = database.command("sql", "INSERT INTO Place SET location = 'POINT (9.19 45.46)'").next();
      database.command("sql", "CREATE INDEX geoPlaceLocation ON Place (location) GEOSPATIAL METADATA {\"precision\": 8}");
      victim.set(inserted.toElement().getIdentity());
    });

    corruptRecordTypeByte(victim.get());

    final ResultSet result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("autoFix") > 0L).isTrue();

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("geoPlaceLocation")).isTrue();
      assertThat(database.getSchema().existsIndex("Place[location]")).isFalse();

      final TypeIndex idx = (TypeIndex) database.getSchema().getIndexByName("geoPlaceLocation");
      assertThat(idx.getType()).isEqualTo(Schema.INDEX_TYPE.GEOSPATIAL);
      final GeoIndexMetadata metadata = (GeoIndexMetadata) idx.getMetadataForNewFile();
      assertThat(metadata.getPrecision()).isEqualTo(8);
    });
  }

  /**
   * Rounds out {@link #fixPreservesNameAndAnalyzerOnFullTextIndex}/{@link #fixPreservesNameAndPrecisionOnGeospatialIndex}
   * with the plain (non-FULL_TEXT/non-GEOSPATIAL) case: {@code RebuildIndexPreservesNameTest} covers this shape for
   * {@code REBUILD INDEX}, but {@code CHECK DATABASE FIX}'s rebuild path had no equivalent coverage of its own for
   * the {@code TypeIndex}-name-loss half of #5934 in isolation from the metadata-loss half.
   */
  @Test
  void fixPreservesExplicitNameOnPlainIndex() {
    final AtomicReference<RID> victim = new AtomicReference<>();
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.name STRING");
      final Result inserted = database.command("sql", "INSERT INTO Doc SET name = 'alpha'").next();
      database.command("sql", "CREATE INDEX myNamedIdx ON Doc (name) NOTUNIQUE");
      victim.set(inserted.toElement().getIdentity());
    });

    corruptRecordTypeByte(victim.get());

    final ResultSet result = database.command("sql", "check database fix");
    assertThat(result.hasNext()).isTrue();
    assertThat(result.next().<Long>getProperty("autoFix") > 0L).isTrue();

    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("myNamedIdx")).isTrue();
      assertThat(database.getSchema().existsIndex("Doc[name]")).isFalse();
      assertThat(database.getSchema().getIndexByName("myNamedIdx").getType()).isEqualTo(Schema.INDEX_TYPE.LSM_TREE);
    });
  }

  /**
   * Overwrites the record-type byte of {@code rid} with a value no {@code RecordFactory} branch knows, so the record
   * still occupies its slot and still has a valid size but cannot be materialised - the precise, page-layout-agnostic
   * corruption shape used by {@code CheckDatabaseRecordScopeTest.corruptRecordTypeByte}.
   */
  private void corruptRecordTypeByte(final RID rid) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
        final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
        final int recordOffset = (int) page.readUnsignedInt(slotOffset);
        assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
        final long[] recordSize = page.readNumberAndSize(recordOffset);
        page.writeByte((int) (recordOffset + recordSize[1]), (byte) 99);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }
}
