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
package com.arcadedb.index.columnar;

import com.arcadedb.engine.block.BlockCorruptionException;
import com.arcadedb.schema.Type;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ColumnarManifestTest {
  @TempDir
  Path dir;

  private KillPoint killPoint;

  @AfterEach
  void disarm() {
    if (killPoint != null)
      killPoint.disarm();
  }

  static ColumnarManifest sample(final long generation, final String externalOwnerMapFile) {
    final ColumnDefinition[] columns = {
        new ColumnDefinition(11, "brand", Type.STRING, ColumnKind.ORDINAL, false, null),
        new ColumnDefinition(12, "tags", Type.LIST, ColumnKind.ORDINAL_LIST, true, null),
        new ColumnDefinition(13, "created", Type.DATE, ColumnKind.LONG, false, ChronoUnit.DAYS) };
    final List<ColumnarManifest.SegmentEntry> segments = List.of(
        new ColumnarManifest.SegmentEntry(1, SegmentFormat.segmentFileName(1), 1000, 0, 0, 0, 7),
        new ColumnarManifest.SegmentEntry(4, SegmentFormat.segmentFileName(4), 250, 3, 17, 8, 9));
    final List<ColumnarManifest.DictionaryEntry> dictionaries = List.of(
        new ColumnarManifest.DictionaryEntry(11, 0, "dict-11.0.dic", 42, 1234L),
        new ColumnarManifest.DictionaryEntry(12, 2, "dict-12.2.dic", 7, 88L));
    return new ColumnarManifest(generation, 65, "Asset_0", 2048, columns, segments, dictionaries, "owners-5.own", externalOwnerMapFile,
        987654321L);
  }

  @Test
  void jsonRoundTrip() {
    final ColumnarManifest manifest = sample(5, "ext-owners-5.own");
    final ColumnarManifest copy = ColumnarManifest.fromJSON(new JSONObject(manifest.toJSON().toString()));
    assertThat(copy.formatVersion()).isEqualTo(1);
    assertThat(copy.generation()).isEqualTo(5);
    assertThat(copy.bucketFileId()).isEqualTo(65);
    assertThat(copy.bucketName()).isEqualTo("Asset_0");
    assertThat(copy.maxRecordsInPage()).isEqualTo(2048);
    assertThat(copy.columns()).containsExactly(manifest.columns());
    assertThat(copy.segments()).isEqualTo(manifest.segments());
    assertThat(copy.dictionaries()).isEqualTo(manifest.dictionaries());
    assertThat(copy.ownerMapFile()).isEqualTo("owners-5.own");
    assertThat(copy.externalOwnerMapFile()).isEqualTo("ext-owners-5.own");
    assertThat(copy.lastTransactionId()).isEqualTo(987654321L);

    final ColumnarManifest next = copy.withGeneration(6);
    assertThat(next.generation()).isEqualTo(6);
    assertThat(next.segments()).isEqualTo(manifest.segments());
    assertThat(copy.generation()).isEqualTo(5);
  }

  @Test
  void filesNameEverySegmentDeadSetDictionaryAndOwnerMap() {
    // deadVersion 0 means the segment has no dead file
    assertThat(sample(1, "ext.own").files()).containsExactlyInAnyOrder(SegmentFormat.segmentFileName(1),
        SegmentFormat.segmentFileName(4), SegmentFormat.deadFileName(4, 3), "dict-11.0.dic", "dict-12.2.dic", "owners-5.own", "ext.own");
    assertThat(sample(1, null).files()).doesNotContainNull().hasSize(6);
  }

  @Test
  void publishIsAtomic() throws Exception {
    sample(1, null).publish(dir);
    assertThat(ColumnarManifest.load(dir).generation()).isEqualTo(1);

    killPoint = KillPoint.arm("publish.manifestTmpWritten");
    final AtomicReference<Throwable> failure = new AtomicReference<>();
    final Thread publisher = new Thread(() -> {
      try {
        sample(2, null).publish(dir);
      } catch (final Throwable t) {
        failure.set(t);
      }
    });
    publisher.start();
    assertThat(killPoint.awaitReached(10_000)).isTrue();
    // the new manifest is complete in its tmp file, but not yet visible
    assertThat(ColumnarManifest.load(dir).generation()).isEqualTo(1);
    killPoint.releaseAsCrashed();
    publisher.join(10_000);
    assertThat(failure.get()).isInstanceOf(ColumnarTestHooks.SimulatedCrash.class);
    assertThat(ColumnarManifest.load(dir).generation()).isEqualTo(1);

    // the crash left the tmp file behind; the next publish replaces it and wins
    killPoint.disarm();
    killPoint = null;
    sample(3, null).publish(dir);
    assertThat(ColumnarManifest.load(dir).generation()).isEqualTo(3);
  }

  @Test
  void manifestRenamedKillPointLeavesTheNewManifest() throws Exception {
    sample(1, null).publish(dir);
    killPoint = KillPoint.arm("publish.manifestRenamed");
    final Thread publisher = new Thread(() -> {
      try {
        sample(2, null).publish(dir);
      } catch (final Throwable ignored) {
        // the simulated crash
      }
    });
    publisher.start();
    assertThat(killPoint.awaitReached(10_000)).isTrue();
    assertThat(ColumnarManifest.load(dir).generation()).isEqualTo(2);
    killPoint.releaseAsCrashed();
    publisher.join(10_000);
  }

  @Test
  void loadOfAnAbsentManifestIsNull() throws IOException {
    assertThat(ColumnarManifest.load(dir)).isNull();
  }

  @Test
  void missingFieldsUseDefaults() {
    final ColumnarManifest manifest = ColumnarManifest.fromJSON(new JSONObject("{\"formatVersion\":1}"));
    assertThat(manifest.generation()).isEqualTo(0);
    assertThat(manifest.segments()).isEmpty();
    assertThat(manifest.dictionaries()).isEmpty();
    assertThat(manifest.columns()).isEmpty();
    assertThat(manifest.ownerMapFile()).isNull();
    assertThat(manifest.externalOwnerMapFile()).isNull();
    assertThat(manifest.files()).isEmpty();
  }

  @Test
  void unknownFormatVersionIsCorruption() throws IOException {
    assertThatThrownBy(() -> ColumnarManifest.fromJSON(new JSONObject("{\"formatVersion\":99}"))).isInstanceOf(
        BlockCorruptionException.class);
    assertThatThrownBy(() -> ColumnarManifest.fromJSON(new JSONObject("{}"))).isInstanceOf(BlockCorruptionException.class);

    Files.writeString(dir.resolve(ColumnarManifest.FILE_NAME), "{\"formatVersion\":99}");
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void badJsonIsCorruption() throws IOException {
    final Path file = dir.resolve(ColumnarManifest.FILE_NAME);
    Files.writeString(file, "{\"formatVersion\":1, \"segments\": [");
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
    Files.writeString(file, "{\"formatVersion\":1, \"segments\": 5}");
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
    Files.write(file, new byte[] { (byte) 0xC3, 0x28 });
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
    Files.writeString(file, "{\"formatVersion\":1, \"columns\": [{\"property\":\"x\"}]}", StandardCharsets.UTF_8);
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
    // a referenced file must stay inside the bucket directory
    Files.writeString(file, "{\"formatVersion\":1, \"segments\": [{\"segmentId\":1, \"file\":\"../seg.acs\"}]}");
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
    Files.writeString(file, "{\"formatVersion\":1, \"ownerMapFile\":\"..\"}");
    assertThatThrownBy(() -> ColumnarManifest.load(dir)).isInstanceOf(BlockCorruptionException.class);
    Files.writeString(file, "{\"formatVersion\":1, \"maxRecordsInPage\":64, \"segments\": [{\"segmentId\":1, \"file\":\"" + SEG_1 + "\"}]}");
    assertThat(ColumnarManifest.load(dir).files()).containsExactly(SEG_1);
  }

  private static final String SEG_1 = SegmentFormat.segmentFileName(1);

  /** A valid manifest with one field replaced; {@code segment} and {@code dictionary} are the entry bodies. */
  private static String manifest(final String top, final String segment, final String dictionary) {
    return "{\"formatVersion\":1, \"maxRecordsInPage\":128" + top + ", \"segments\":[{" + segment + "}], \"dictionaries\":[{" + dictionary
        + "}]}";
  }

  private static final String SEGMENT    = "\"segmentId\":1, \"file\":\"" + SEG_1 + "\", \"rows\":10, \"deadVersion\":1, \"deadRows\":2, "
      + "\"minPage\":0, \"maxPage\":3";
  private static final String DICTIONARY = "\"indexFileId\":11, \"generation\":0, \"file\":\"dict-11.0.dic\", \"entries\":5, \"bytes\":64";

  private static void assertCorrupt(final String json) {
    assertThatThrownBy(() -> ColumnarManifest.fromJSON(new JSONObject(json))).as(json).isInstanceOf(BlockCorruptionException.class);
  }

  @Test
  void formatVersionMustBeExactlyOne() {
    assertThat(ColumnarManifest.fromJSON(new JSONObject("{\"formatVersion\":1.0}")).formatVersion()).isEqualTo(1);
    assertCorrupt("{\"formatVersion\":\"x\"}");
    assertCorrupt("{\"formatVersion\":\"1\"}");
    assertCorrupt("{\"formatVersion\":1.9}");
    assertCorrupt("{\"formatVersion\":4294967297}");
    assertCorrupt("{\"formatVersion\":-1}");
    assertCorrupt("{\"formatVersion\":true}");
    assertCorrupt("{\"formatVersion\":null}");
  }

  @Test
  void outOfRangeValuesAreCorruption() {
    final ColumnarManifest valid = ColumnarManifest.fromJSON(new JSONObject(manifest("", SEGMENT, DICTIONARY)));
    assertThat(valid.segments().getFirst().deadFile()).isEqualTo(SegmentFormat.deadFileName(1, 1));
    assertThat(valid.dictionaries().getFirst().bytes()).isEqualTo(64);

    // maxRecordsInPage: positive multiple of 64 up to 65536, 0 only without segments
    for (final String bad : new String[] { "0", "-64", "100", "131072", "64.5" })
      assertCorrupt(manifest("", SEGMENT, DICTIONARY).replace("\"maxRecordsInPage\":128", "\"maxRecordsInPage\":" + bad));
    assertThat(ColumnarManifest.fromJSON(new JSONObject("{\"formatVersion\":1, \"maxRecordsInPage\":65536}")).maxRecordsInPage())
        .isEqualTo(65536);
    // top-level fields
    assertCorrupt(manifest(", \"generation\":-1", SEGMENT, DICTIONARY));
    assertCorrupt(manifest(", \"bucketFileId\":-1", SEGMENT, DICTIONARY));
    assertCorrupt(manifest(", \"generation\":1.5", SEGMENT, DICTIONARY));
    // segment fields
    assertCorrupt(manifest("", SEGMENT.replace("\"segmentId\":1", "\"segmentId\":-1"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"rows\":10", "\"rows\":-10"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"deadRows\":2", "\"deadRows\":-2"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"deadRows\":2", "\"deadRows\":11"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"deadVersion\":1", "\"deadVersion\":-1"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"minPage\":0", "\"minPage\":-1"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"minPage\":0", "\"minPage\":4"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"segmentId\":1", "\"segmentId\":2"), DICTIONARY));
    assertCorrupt(manifest("", SEGMENT.replace("\"segmentId\":1, ", ""), DICTIONARY));
    // dictionary fields
    assertCorrupt(manifest("", SEGMENT, DICTIONARY.replace("\"entries\":5", "\"entries\":-5")));
    assertCorrupt(manifest("", SEGMENT, DICTIONARY.replace("\"bytes\":64", "\"bytes\":-64")));
    assertCorrupt(manifest("", SEGMENT, DICTIONARY.replace("\"generation\":0", "\"generation\":-1")));
    assertCorrupt(manifest("", SEGMENT, DICTIONARY.replace("\"indexFileId\":11", "\"indexFileId\":-11")));
    assertCorrupt(manifest("", SEGMENT, DICTIONARY.replace("\"entries\":5", "\"entries\":4294967301")));
  }

  @Test
  void duplicateFilesAreCorruption() {
    // the same segment twice
    assertCorrupt("{\"formatVersion\":1, \"maxRecordsInPage\":128, \"segments\":[{" + SEGMENT + "}, {" + SEGMENT + "}]}");
    // two dictionaries of one index, and two indexes sharing a file
    assertCorrupt(manifest("", SEGMENT, DICTIONARY + "}, {" + DICTIONARY.replace("dict-11.0.dic", "dict-11.1.dic")));
    assertCorrupt(manifest("", SEGMENT, DICTIONARY + "}, {" + DICTIONARY.replace("\"indexFileId\":11", "\"indexFileId\":12")));
    // an owner map that is also a dictionary file
    assertCorrupt(manifest(", \"ownerMapFile\":\"dict-11.0.dic\"", SEGMENT, DICTIONARY));
  }
}
