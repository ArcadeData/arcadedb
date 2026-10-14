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
package com.arcadedb.index.vector;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.TypeIndex;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9917: the side files of a persisted vector graph are observed before a point in time and accepted after it only
 * when nothing replaced them in between, so a full backup can archive them with the graph's pages and the restored
 * database can verify and reuse the graph instead of rebuilding it. See {@link LSMVectorIndexGraphSideFiles} for why
 * that pairing is sound; these tests drive each rule of it through the real writers.
 */
@Tag("vector")
class Issue9917GraphSideFilesTest extends TestHelper {
  private static final int DIMENSIONS = 16;
  private static final int VECTORS    = 400;

  @Test
  void anIdleGraphIsVouchedForByItsManifestItsOrdinalMapAndItsPQ() throws Exception {
    createVectorIndex("Doc", "NONE");
    createVectorIndex("PqDoc", "PRODUCT");

    final Map<Integer, LSMVectorIndexGraphSideFiles> captured = LSMVectorIndexGraphSideFiles.capture((DatabaseInternal) database);
    assertThat(captured).as("both graphs are idle, so both are vouched for")
        .containsOnlyKeys(graphFile("Doc").getFileId(), graphFile("PqDoc").getFileId());

    try (final LSMVectorIndexGraphSideFiles.Opened opened = captured.get(graphFile("Doc").getFileId()).openIfUnchanged()) {
      assertThat(opened).isNotNull();
      assertThat(names(opened)).as("a non-quantized index carries no PQ, and the manifest comes last")
          .containsExactly(ordinalMapName("Doc"), manifestName("Doc"));
    }

    final Path pq = vectorIndex("PqDoc").getProductQuantizationFilePath();
    assertThat(pq).as("the fixture really is PRODUCT-quantized, with its PQ on disk").isNotNull();
    assertThat(Files.exists(pq)).isTrue();
    try (final LSMVectorIndexGraphSideFiles.Opened opened = captured.get(graphFile("PqDoc").getFileId()).openIfUnchanged()) {
      assertThat(opened).isNotNull();
      // NO ORDINAL MAP: a PRODUCT-quantized index never records one (writeGraphManifest), and one absent at the capture
      // has to be absent at the open too, which it is
      assertThat(names(opened)).as("a PRODUCT-quantized index carries its PQ, and no ordinal map")
          .containsExactly(pq.getFileName().toString(), manifestName("PqDoc"));
    }
  }

  @Test
  void aPersistBetweenTheCaptureAndTheOpenRefusesTheSideFiles() throws Exception {
    createVectorIndex("Doc", "NONE");
    final LSMVectorIndexGraphSideFiles sideFiles = LSMVectorIndexGraphSideFiles.capture(graphFile("Doc"));
    assertThat(sideFiles).isNotNull();

    // THE RACE THE PROTOCOL EXISTS FOR: the pages captured at t0 are no longer the ones the manifest on disk describes
    vectorIndex("Doc").buildVectorGraphNow();

    assertThat(sideFiles.openIfUnchanged()).as("a manifest replaced since the capture cannot vouch for the pages at t0")
        .isNull();
  }

  @Test
  void aGraphWithNoManifestIsNotVouchedFor() {
    createVectorIndex("Doc", "NONE");
    // THE STATE OF A PERSIST IN FLIGHT: writeGraph drops the manifest before it touches the first page
    graphFile("Doc").getManifest().invalidate();

    assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("Doc"))).isNull();
    assertThat(LSMVectorIndexGraphSideFiles.capture((DatabaseInternal) database)).isEmpty();
  }

  @Test
  void aPQOlderThanTheManifestIsRefusedBecauseItBelongsToAnEarlierGraph() {
    createVectorIndex("Doc", "NONE");
    createVectorIndex("PqDoc", "PRODUCT");

    // A MANIFEST WRITTEN AFTER THE PQ: the order a persist leaves behind between certifying a new graph and replacing
    // the previous graph's PQ. markCloseDeferred rewrites the manifest without touching anything else
    graphFile("PqDoc").getManifest().markCloseDeferred();
    graphFile("Doc").getManifest().markCloseDeferred();

    assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("PqDoc"))).as("the PQ predates the manifest").isNull();
    assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("Doc"))).as("an index with no PQ is not affected")
        .isNotNull();
  }

  @Test
  void aPQMissingFromAProductQuantizedIndexIsRefused() {
    createVectorIndex("PqDoc", "PRODUCT");
    assertThat(vectorIndex("PqDoc").getProductQuantizationFilePath().toFile().delete()).isTrue();

    assertThat(LSMVectorIndexGraphSideFiles.capture(graphFile("PqDoc")))
        .as("the restored index would rebuild its graph for want of a PQ anyway").isNull();
  }

  @Test
  void filesOpenedWhileUnchangedKeepTheirBytesWhenAPersistReplacesThemDuringTheCopy() throws Exception {
    // BOTH SHAPES: the non-quantized index has an ordinal map, the PRODUCT-quantized one a PQ
    createVectorIndex("Doc", "NONE");
    createVectorIndex("PqDoc", "PRODUCT");
    final Path directory = Path.of(database.getDatabasePath());
    for (final String type : new String[] { "Doc", "PqDoc" }) {
      final LSMVectorIndexGraphSideFiles sideFiles = LSMVectorIndexGraphSideFiles.capture(graphFile(type));
      assertThat(sideFiles).isNotNull();

      final Map<String, byte[]> before = new HashMap<>();
      try (final LSMVectorIndexGraphSideFiles.Opened opened = sideFiles.openIfUnchanged()) {
        assertThat(opened).isNotNull();
        assertThat(opened.entries()).hasSize(2);
        for (final LSMVectorIndexGraphSideFiles.Entry entry : opened.entries())
          before.put(entry.fileName(), Files.readAllBytes(directory.resolve(entry.fileName())));

        // EVERY SIDE FILE IS REPLACED WHILE THE STREAMS ARE OPEN, by a persist over MORE vectors, so that what replaces
        // them cannot be byte-identical to what they held
        insertVectors(type, VECTORS, 50);
        vectorIndex(type).buildVectorGraphNow();
        for (final String name : before.keySet())
          assertThat(Files.readAllBytes(directory.resolve(name)))
              .as("the fixture: the persist really did replace %s", name).isNotEqualTo(before.get(name));
        assertThat(sideFiles.openIfUnchanged()).as("and the old observation no longer matches").isNull();

        for (final LSMVectorIndexGraphSideFiles.Entry entry : opened.entries())
          try (final InputStream input = entry.input()) {
            assertThat(input.readAllBytes()).as("%s: '%s' is read as it was when it was opened", type, entry.fileName())
                .isEqualTo(before.get(entry.fileName()));
          }
      }
    }
  }

  // ------------------------------------------------------------------------------------------------------- HELPERS

  private void createVectorIndex(final String type, final String quantization) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE " + type);
      database.command("sql", "CREATE PROPERTY " + type + ".embedding ARRAY_OF_FLOATS");
    });
    database.command("sql", "CREATE INDEX ON `" + type + "` (`embedding`) LSM_VECTOR METADATA {\"dimensions\":" + DIMENSIONS
        + ",\"similarity\":\"COSINE\",\"quantization\":\"" + quantization + "\"}");
    insertVectors(type, 0, VECTORS);
    vectorIndex(type).buildVectorGraphNow();
  }

  private void insertVectors(final String type, final int from, final int count) {
    database.transaction(() -> {
      for (int i = from; i < from + count; i++) {
        final float[] vector = new float[DIMENSIONS];
        for (int j = 0; j < DIMENSIONS; j++)
          vector[j] = (float) Math.sin(i * 0.37 + j * 1.13) + 1.5f;
        database.newDocument(type).set("embedding", vector).save();
      }
    });
  }

  private LSMVectorIndex vectorIndex(final String type) {
    return vectorIndexOf(database, type);
  }

  private static LSMVectorIndex vectorIndexOf(final Database db, final String type) {
    return (LSMVectorIndex) ((TypeIndex) db.getSchema().getIndexByName(type + "[embedding]")).getIndexesOnBuckets()[0];
  }

  private LSMVectorIndexGraphFile graphFile(final String type) {
    final LSMVectorIndexGraphFile graphFile = vectorIndex(type).getGraphFile();
    assertThat(graphFile).as("the index must have persisted its graph").isNotNull();
    return graphFile;
  }

  private String manifestName(final String type) {
    return graphFile(type).getManifest().getFilePath().getFileName().toString();
  }

  private String ordinalMapName(final String type) {
    return graphFile(type).getManifest().getOrdinalMapFilePath().getFileName().toString();
  }

  private static List<String> names(final LSMVectorIndexGraphSideFiles.Opened opened) {
    final List<String> names = new ArrayList<>();
    for (final LSMVectorIndexGraphSideFiles.Entry entry : opened.entries())
      names.add(entry.fileName());
    return names;
  }
}
