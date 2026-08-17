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
package com.arcadedb.engine;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #6314, items 1 and 2 - what a component is allowed to believe about the file it was handed.
 * <p>
 * A {@code PaginatedComponent} addresses its file by two numbers it does not read off the file: the file id, and the
 * page size that turns a page number into an offset. #6283 established the first - the component asserts that the file
 * it was handed carries the id it was built with - and this is the rest of the same invariant:
 * <ul>
 * <li><b>Item 1</b>: the two TimeSeries components DISCARDED the page size and version {@code ComponentFactory} parses
 * off the file's own name and re-derived the page size from {@code arcadedb.bucketDefaultPageSize} instead, so a
 * database whose configuration changed between two runs reopened its {@code .tstb}/{@code .tstd} files at a stride they
 * were never written with. Nothing downstream could catch it: {@code PageManager} resolves a page with the CALLER's
 * page size, so the result is a misaligned read of real bytes rather than an error.</li>
 * <li><b>Item 2</b>: the by-ID {@code FileManager.getOrCreateFile} handed back whatever file was registered under the
 * id without ever looking at the path it was asked for. No live defect - its one production caller, the HA follower's
 * file-creation path, checks exactly this itself (#6063) - but the invariant belonged to the API rather than to a
 * caller who has to know to re-implement it.</li>
 * </ul>
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue6314ComponentFileAgreementTest extends TestHelper {
  private static final String TYPE       = "SensorReading";
  private static final int    CREATED_AT = 65_536;
  private static final int    REOPENED_AT = 16_384;
  private static final int    POINTS     = 200;

  /**
   * Item 1, end to end: a TimeSeries type created under one {@code bucketDefaultPageSize} and reopened under another.
   * Its components must come back at the page size their FILES were written with - the setting is
   * {@code SCOPE.DATABASE} and user-settable, so this needs no bug to reach, only a configuration change between two
   * runs - and every point must still read back.
   */
  @Test
  void aTimeSeriesTypeReopensAtThePageSizeItsFilesWereWrittenWith() {
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(CREATED_AT);

    database.command("sql",
        "CREATE TIMESERIES TYPE " + TYPE + " TIMESTAMP ts TAGS (sensor_id STRING) FIELDS (temperature DOUBLE)");
    database.transaction(() -> {
      for (int i = 0; i < POINTS; i++)
        database.command("sql", "INSERT INTO " + TYPE + " SET ts = " + (1_000 + i) + ", sensor_id = 'A', temperature = "
            + (20.0 + i));
    });

    final List<String> componentsBefore = timeSeriesComponentNames();
    assertThat(componentsBefore).as("the fixture must have both a .tstb and a .tstd to reopen").hasSizeGreaterThan(1);
    assertThat(pageSizesOf(componentsBefore)).as("every TimeSeries component must have been created at " + CREATED_AT)
        .containsOnly(CREATED_AT);

    // A configuration change between two runs, which is all it takes.
    final String path = database.getDatabasePath();
    database.close();
    GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE.setValue(REOPENED_AT);
    database = new DatabaseFactory(path).open();

    assertThat(database.getConfiguration().getValueAsInteger(GlobalConfiguration.BUCKET_DEFAULT_PAGE_SIZE))
        .as("the reopened database really must disagree with the files, or this test proves nothing")
        .isEqualTo(REOPENED_AT);
    assertThat(pageSizesOf(timeSeriesComponentNames()))
        .as("the components must address their files at the stride the files were written with, not at today's default")
        .containsOnly(CREATED_AT);

    final List<Object> temperatures = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", "SELECT FROM " + TYPE + " ORDER BY ts")) {
      while (rs.hasNext())
        temperatures.add(rs.next().getProperty("temperature"));
    }
    assertThat(temperatures).as("and every point must read back").hasSize(POINTS);
    assertThat(((Number) temperatures.getFirst()).doubleValue()).isEqualTo(20.0);
    assertThat(((Number) temperatures.getLast()).doubleValue()).isEqualTo(20.0 + POINTS - 1);
  }

  /**
   * The invariant behind item 1, asserted where it belongs rather than left to every component's constructor to
   * remember: a component built with a page size its file does not have fails at once, loudly, before it addresses a
   * single page.
   */
  @Test
  void aComponentBuiltOnAFileOfAnotherPageSizeIsRefused() {
    final DatabaseInternal db = (DatabaseInternal) database;

    database.transaction(() -> database.getSchema().createDocumentType("Plain", 1));
    final LocalBucket bucket = (LocalBucket) database.getSchema().getType("Plain").getBuckets(false).getFirst();

    assertThatThrownBy(() -> new LocalBucket(db, bucket.getName(), bucket.getComponentFile().getFilePath(),
        bucket.getFileId(), ComponentFile.MODE.READ_WRITE, bucket.getPageSize() * 2, 0))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("page size")
        .hasMessageContaining(bucket.getName());
  }

  /**
   * Item 2: the by-ID overload answers for the file the caller NAMED, or it throws. A {@code SchemaException} and not
   * some new type: the HA apply path quarantines the database and resyncs from a snapshot on exactly this exception
   * (#6063), and a repair that depends on the type must not be lost by pushing the check down.
   */
  @Test
  void theByIdGetOrCreateFileRefusesAnIdThatNamesAnotherFile() throws Exception {
    final DatabaseInternal db = (DatabaseInternal) database;
    final FileManager fileManager = db.getFileManager();

    final int fileId = 9101;
    final String registered = "issue6314_registered." + fileId + ".65536.v0." + LocalBucket.BUCKET_EXT;
    fileManager.getOrCreateFile(fileId, db.getDatabasePath() + File.separator + registered);

    assertThat(fileManager.getOrCreateFile(fileId, db.getDatabasePath() + File.separator + registered).getFileName())
        .as("asking again for the very same file is the idempotent case and must still answer").isEqualTo(registered);

    final String other = "issue6314_other." + fileId + ".65536.v0." + LocalBucket.BUCKET_EXT;
    assertThatThrownBy(() -> fileManager.getOrCreateFile(fileId, db.getDatabasePath() + File.separator + other))
        .isInstanceOf(SchemaException.class)
        .hasMessageContaining(String.valueOf(fileId))
        .hasMessageContaining(registered)
        .hasMessageContaining(other);
  }

  /** The component names of the TimeSeries type's own files: its shard buckets and its tag dictionary. */
  private List<String> timeSeriesComponentNames() {
    final LocalTimeSeriesType type = (LocalTimeSeriesType) database.getSchema().getType(TYPE);
    assertThat(type).isNotNull();

    final List<String> names = new ArrayList<>();
    for (final ComponentFile file : ((DatabaseInternal) database).getFileManager().getFiles())
      if (file != null && file.getComponentName().startsWith(TYPE))
        names.add(file.getComponentName());
    return names;
  }

  /** The page size each of those components is ADDRESSING its file with, which is the number under test. */
  private List<Integer> pageSizesOf(final List<String> componentNames) {
    final List<Integer> pageSizes = new ArrayList<>();
    for (final String name : componentNames) {
      final Component component = ((DatabaseInternal) database).getSchema().getEmbedded().getFileByName(name);
      assertThat(component).as("component '%s' must be registered in the schema", name).isNotNull();
      pageSizes.add(((PaginatedComponent) component).getPageSize());
    }
    return pageSizes;
  }
}
