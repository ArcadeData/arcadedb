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
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The published state of one bucket's projection (spec 5.5): its columns, the live segments with the dead set version of each, the
 * dictionary generation and durable length of each column, and the owner map files. Immutable. Published as
 * {@code <bucketDir>/manifest.json} with one atomic replace, after every file it names is durable, so a crash leaves either the
 * previous manifest or this one, each naming only complete files.
 * <p>
 * A segment entry with {@code deadVersion} 0 has no dead set file (no row of it is dead); any other version names
 * {@code seg-<id>.dead.<version>}. Whether a no-WAL write is pending is not recorded here: the {@code NOWAL} marker file is the only
 * record of it.
 */
public final class ColumnarManifest {
  public static final String FILE_NAME           = "manifest.json";
  static final        int    FORMAT_VERSION      = 1;
  /** Upper bound of a bucket's slots per page accepted from a manifest. */
  static final        int    MAX_RECORDS_IN_PAGE = 65536;

  public record SegmentEntry(long segmentId, String file, long rows, int deadVersion, long deadRows, int minPage, int maxPage) {
    /** The dead set file name, null when {@code deadVersion} is 0. */
    public String deadFile() {
      return deadVersion == 0 ? null : SegmentFormat.deadFileName(segmentId, deadVersion);
    }
  }

  /**
   * @param entries the durable entry count and {@code bytes} the durable length of the generation file, both from one
   *                {@link GlobalDictionary#fsync()} {@link GlobalDictionary.DurableState}
   */
  public record DictionaryEntry(int indexFileId, int generation, String file, int entries, long bytes) {
  }

  private final long                  generation;
  private final int                   bucketFileId;
  private final String                bucketName;
  private final int                   maxRecordsInPage;
  private final ColumnDefinition[]    columns;
  private final List<SegmentEntry>    segments;
  private final List<DictionaryEntry> dictionaries;
  private final String                ownerMapFile;
  private final String                externalOwnerMapFile;
  private final long                  lastTransactionId;
  private final Set<String>           files;

  /**
   * @param ownerMapFile         the content owner map of this generation, null for none
   * @param externalOwnerMapFile the owner map of the paired external-property bucket, null when the bucket has none
   * @param lastTransactionId    the database's last transaction id when published; informational only
   */
  public ColumnarManifest(final long generation, final int bucketFileId, final String bucketName, final int maxRecordsInPage,
      final ColumnDefinition[] columns, final List<SegmentEntry> segments, final List<DictionaryEntry> dictionaries, final String ownerMapFile,
      final String externalOwnerMapFile, final long lastTransactionId) {
    this.generation = generation;
    this.bucketFileId = bucketFileId;
    this.bucketName = bucketName;
    this.maxRecordsInPage = maxRecordsInPage;
    this.columns = columns.clone();
    this.segments = List.copyOf(segments);
    this.dictionaries = List.copyOf(dictionaries);
    this.ownerMapFile = ownerMapFile;
    this.externalOwnerMapFile = externalOwnerMapFile;
    this.lastTransactionId = lastTransactionId;

    final Set<String> names = new HashSet<>();
    for (final SegmentEntry segment : this.segments) {
      names.add(segment.file());
      if (segment.deadVersion() != 0)
        names.add(segment.deadFile());
    }
    for (final DictionaryEntry dictionary : this.dictionaries)
      names.add(dictionary.file());
    if (ownerMapFile != null)
      names.add(ownerMapFile);
    if (externalOwnerMapFile != null)
      names.add(externalOwnerMapFile);
    this.files = Set.copyOf(names);
  }

  public ColumnarManifest withGeneration(final long next) {
    return new ColumnarManifest(next, bucketFileId, bucketName, maxRecordsInPage, columns, segments, dictionaries, ownerMapFile,
        externalOwnerMapFile, lastTransactionId);
  }

  public int formatVersion() {
    return FORMAT_VERSION;
  }

  public long generation() {
    return generation;
  }

  public int bucketFileId() {
    return bucketFileId;
  }

  /** The bucket name when published; informational, the directory is keyed by {@link #bucketFileId()}. */
  public String bucketName() {
    return bucketName;
  }

  public int maxRecordsInPage() {
    return maxRecordsInPage;
  }

  /** A copy of the column definitions. */
  public ColumnDefinition[] columns() {
    return columns.clone();
  }

  public List<SegmentEntry> segments() {
    return segments;
  }

  public List<DictionaryEntry> dictionaries() {
    return dictionaries;
  }

  public String ownerMapFile() {
    return ownerMapFile;
  }

  public String externalOwnerMapFile() {
    return externalOwnerMapFile;
  }

  public long lastTransactionId() {
    return lastTransactionId;
  }

  /** Every file name this manifest references (segments, dead sets, dictionaries, owner maps), for {@code deleteUnreferenced}. */
  public Set<String> files() {
    return files;
  }

  public JSONObject toJSON() {
    final JSONObject json = new JSONObject();
    json.put("formatVersion", FORMAT_VERSION);
    json.put("generation", generation);
    json.put("bucketFileId", bucketFileId);
    if (bucketName != null)
      json.put("bucketName", bucketName);
    json.put("maxRecordsInPage", maxRecordsInPage);
    final JSONArray columnsJson = new JSONArray();
    for (final ColumnDefinition column : columns)
      columnsJson.put(column.toJSON());
    json.put("columns", columnsJson);
    final JSONArray segmentsJson = new JSONArray();
    for (final SegmentEntry segment : segments)
      segmentsJson.put(new JSONObject().put("segmentId", segment.segmentId()).put("file", segment.file()).put("rows", segment.rows())
          .put("deadVersion", segment.deadVersion()).put("deadRows", segment.deadRows()).put("minPage", segment.minPage())
          .put("maxPage", segment.maxPage()));
    json.put("segments", segmentsJson);
    final JSONArray dictionariesJson = new JSONArray();
    for (final DictionaryEntry dictionary : dictionaries)
      dictionariesJson.put(new JSONObject().put("indexFileId", dictionary.indexFileId()).put("generation", dictionary.generation())
          .put("file", dictionary.file()).put("entries", dictionary.entries()).put("bytes", dictionary.bytes()));
    json.put("dictionaries", dictionariesJson);
    if (ownerMapFile != null)
      json.put("ownerMapFile", ownerMapFile);
    if (externalOwnerMapFile != null)
      json.put("externalOwnerMapFile", externalOwnerMapFile);
    json.put("lastTransactionId", lastTransactionId);
    return json;
  }

  /**
   * Absent optional fields take their defaults (generation 0, no segments, no dictionaries, no owner maps).
   *
   * @throws BlockCorruptionException when {@code formatVersion} is absent or not exactly 1, a field has the wrong type or a
   *                                  non-integral or out-of-range value, a required entry field (segment id and file, dictionary index
   *                                  file id and file, column definition fields) is missing, a segment file is not named after its id,
   *                                  two entries name the same file or index, or a file name is not a plain name inside the bucket
   *                                  directory
   */
  public static ColumnarManifest fromJSON(final JSONObject json) {
    try {
      final long version = integer(json, "formatVersion", -1, 0, Integer.MAX_VALUE);
      if (version != FORMAT_VERSION)
        throw new BlockCorruptionException("Columnar manifest has format version " + version + ", expected " + FORMAT_VERSION);

      final JSONArray columnsJson = json.getJSONArray("columns", new JSONArray());
      final ColumnDefinition[] columns = new ColumnDefinition[columnsJson.length()];
      for (int i = 0; i < columns.length; i++)
        columns[i] = ColumnDefinition.fromJSON(columnsJson.getJSONObject(i));

      final JSONArray segmentsJson = json.getJSONArray("segments", new JSONArray());
      final List<SegmentEntry> segments = new ArrayList<>(segmentsJson.length());
      for (int i = 0; i < segmentsJson.length(); i++) {
        final JSONObject s = segmentsJson.getJSONObject(i);
        final long segmentId = integer(s, "segmentId", -1, 0, Long.MAX_VALUE);
        final String file = s.getString("file");
        if (!file.equals(SegmentFormat.segmentFileName(segmentId)))
          throw new BlockCorruptionException("Columnar manifest segment " + segmentId + " names file '" + file + "'");
        final long rows = integer(s, "rows", 0, 0, Long.MAX_VALUE);
        final long deadRows = integer(s, "deadRows", 0, 0, rows);
        final int minPage = (int) integer(s, "minPage", 0, 0, Integer.MAX_VALUE);
        final int maxPage = (int) integer(s, "maxPage", 0, minPage, Integer.MAX_VALUE);
        segments.add(new SegmentEntry(segmentId, file, rows, (int) integer(s, "deadVersion", 0, 0, Integer.MAX_VALUE), deadRows, minPage,
            maxPage));
      }

      final JSONArray dictionariesJson = json.getJSONArray("dictionaries", new JSONArray());
      final List<DictionaryEntry> dictionaries = new ArrayList<>(dictionariesJson.length());
      final Set<Integer> indexFileIds = new HashSet<>();
      for (int i = 0; i < dictionariesJson.length(); i++) {
        final JSONObject d = dictionariesJson.getJSONObject(i);
        final int indexFileId = (int) integer(d, "indexFileId", -1, 0, Integer.MAX_VALUE);
        if (!indexFileIds.add(indexFileId))
          throw new BlockCorruptionException("Columnar manifest holds two dictionaries of index " + indexFileId);
        dictionaries.add(new DictionaryEntry(indexFileId, (int) integer(d, "generation", 0, 0, Integer.MAX_VALUE), fileName(d.getString("file")),
            (int) integer(d, "entries", 0, 0, Integer.MAX_VALUE), integer(d, "bytes", 0, 0, Long.MAX_VALUE)));
      }

      final int maxRecordsInPage = (int) integer(json, "maxRecordsInPage", 0, 0, MAX_RECORDS_IN_PAGE);
      // 0 only for a manifest without segments, which never builds a bitmap
      if (maxRecordsInPage % 64 != 0 || (maxRecordsInPage == 0 && !segments.isEmpty()))
        throw new BlockCorruptionException("Columnar manifest has invalid maxRecordsInPage " + maxRecordsInPage);

      final String ownerMapFile = json.getString("ownerMapFile", null);
      final String externalOwnerMapFile = json.getString("externalOwnerMapFile", null);
      final ColumnarManifest manifest = new ColumnarManifest(integer(json, "generation", 0, 0, Long.MAX_VALUE),
          (int) integer(json, "bucketFileId", 0, 0, Integer.MAX_VALUE), json.getString("bucketName", null), maxRecordsInPage, columns, segments,
          dictionaries, ownerMapFile == null ? null : fileName(ownerMapFile), externalOwnerMapFile == null ? null : fileName(externalOwnerMapFile),
          integer(json, "lastTransactionId", 0, Long.MIN_VALUE, Long.MAX_VALUE));

      int named = dictionaries.size() + (ownerMapFile == null ? 0 : 1) + (externalOwnerMapFile == null ? 0 : 1);
      for (final SegmentEntry segment : segments)
        named += segment.deadVersion() == 0 ? 1 : 2;
      if (manifest.files().size() != named)
        throw new BlockCorruptionException("Columnar manifest names the same file twice");
      return manifest;
    } catch (final BlockCorruptionException e) {
      throw e;
    } catch (final RuntimeException e) {
      throw new BlockCorruptionException("Columnar manifest is malformed: " + e.getMessage());
    }
  }

  /**
   * An integral JSON number within [min, max]; {@code defaultValue} when absent, and a required field (default outside the range)
   * is refused when absent. Never truncates: 1.9 or 2^32 + 1 is refused, not read as 1.
   */
  private static long integer(final JSONObject json, final String field, final long defaultValue, final long min, final long max) {
    final Object value = json.get(field, null);
    if (value == null) {
      if (defaultValue < min || defaultValue > max)
        throw new BlockCorruptionException("Columnar manifest misses the required field '" + field + "'");
      return defaultValue;
    }
    if (!(value instanceof Number number))
      throw new BlockCorruptionException("Columnar manifest field '" + field + "' is not a number: " + value);
    final BigDecimal exact = new BigDecimal(number.toString());
    if (exact.compareTo(BigDecimal.valueOf(min)) < 0 || exact.compareTo(BigDecimal.valueOf(max)) > 0)
      throw new BlockCorruptionException("Columnar manifest field '" + field + "' is out of range [" + min + ", " + max + "]: " + value);
    try {
      // within [min, max], so only a fractional part can make it inexact
      return exact.longValueExact();
    } catch (final ArithmeticException e) {
      throw new BlockCorruptionException("Columnar manifest field '" + field + "' is not an integer: " + value);
    }
  }

  /**
   * Publishes this manifest as {@code <bucketDir>/manifest.json}. The caller has written and fsynced every file it names; the
   * directory is fsynced first so those files' entries are durable before the rename that makes the manifest name them.
   */
  public void publish(final Path bucketDir) throws IOException {
    writeTmp(bucketDir);
    rename(bucketDir);
  }

  /**
   * First half of {@link #publish}: fsyncs the directory, then writes and forces {@code manifest.json.tmp}. The published manifest is
   * untouched, so a failure here changes nothing a reader or a crash recovery sees.
   */
  void writeTmp(final Path bucketDir) throws IOException {
    ColumnarStore.fsyncDirectory(bucketDir);
    ColumnarStore.writeTmp(bucketDir.resolve(FILE_NAME), toJSON().toString().getBytes(StandardCharsets.UTF_8), "publish.manifestTmpWritten");
  }

  /** Second half of {@link #publish}: replaces {@code manifest.json} with the tmp file of {@link #writeTmp} and fsyncs the directory. */
  void rename(final Path bucketDir) throws IOException {
    ColumnarStore.renameTmp(bucketDir.resolve(FILE_NAME), "publish.manifestRenamed");
  }

  /**
   * @return the published manifest, null when the bucket directory has none
   *
   * @throws BlockCorruptionException when the file is not valid JSON or fails a {@link #fromJSON} check
   */
  public static ColumnarManifest load(final Path bucketDir) throws IOException {
    final byte[] bytes;
    try {
      bytes = Files.readAllBytes(bucketDir.resolve(FILE_NAME));
    } catch (final NoSuchFileException e) {
      return null;
    }
    final JSONObject json;
    try {
      json = new JSONObject(new String(bytes, StandardCharsets.UTF_8));
    } catch (final RuntimeException e) {
      throw new BlockCorruptionException("Columnar manifest in " + bucketDir + " is not valid JSON: " + e.getMessage());
    }
    return fromJSON(json);
  }

  /** A referenced file must be a plain name inside the bucket directory, so a damaged manifest cannot point a reader elsewhere. */
  private static String fileName(final String name) {
    if (name.isEmpty() || name.equals(".") || name.equals("..") || name.indexOf('/') >= 0 || name.indexOf('\\') >= 0)
      throw new BlockCorruptionException("Columnar manifest names an invalid file '" + name + "'");
    return name;
  }
}
