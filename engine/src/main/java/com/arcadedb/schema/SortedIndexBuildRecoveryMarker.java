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

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.ComponentFile;
import com.arcadedb.index.IndexException;
import com.arcadedb.log.LogManager;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Level;
import java.util.stream.Stream;

/** Durable cleanup manifest for sorted builds whose component pages are written outside the WAL. */
final class SortedIndexBuildRecoveryMarker {
  static final String FILE_PREFIX = ".arcadedb-sorted-index-build-";
  static final String FILE_SUFFIX = ".json";
  private static final String SORT_DIRECTORY_PREFIX = ".arcadedb-index-sort-";

  private final Path markerPath;
  private final Path spillWorkspace;
  private final Set<Integer> baselineFileIds;

  private SortedIndexBuildRecoveryMarker(final Path markerPath, final Path spillWorkspace,
      final Set<Integer> baselineFileIds) {
    this.markerPath = markerPath;
    this.spillWorkspace = spillWorkspace;
    this.baselineFileIds = Set.copyOf(baselineFileIds);
  }

  static SortedIndexBuildRecoveryMarker create(final DatabaseInternal database, final String typeName,
      final List<String> propertyNames, final Path configuredSpillParent) {
    final String buildId = UUID.randomUUID().toString();
    final Path databasePath = Path.of(database.getDatabasePath()).toAbsolutePath().normalize();
    final Path spillParent = (configuredSpillParent != null ? configuredSpillParent : databasePath)
        .toAbsolutePath().normalize();
    final Path spillWorkspace = spillParent.resolve(SORT_DIRECTORY_PREFIX + buildId);

    final JSONArray baselineFileIds = new JSONArray();
    final Set<Integer> baselineFileIdSet = new HashSet<>();
    for (final ComponentFile file : database.getFileManager().getFiles())
      if (file != null) {
        baselineFileIds.put(file.getFileId());
        baselineFileIdSet.add(file.getFileId());
      }

    final JSONObject marker = new JSONObject();
    marker.put("version", 1);
    marker.put("typeName", typeName);
    marker.put("propertyNames", new JSONArray(propertyNames));
    marker.put("startedUtc", Instant.now().toString());
    marker.put("baselineFileIds", baselineFileIds);
    marker.put("spillWorkspace", spillWorkspace.toString());

    final Path markerPath = databasePath.resolve(FILE_PREFIX + buildId + FILE_SUFFIX);
    try {
      Files.createDirectories(spillParent);
      Files.writeString(markerPath, marker.toString(), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW,
          StandardOpenOption.WRITE);
      forceFile(markerPath);
      forceDirectory(databasePath);
      return new SortedIndexBuildRecoveryMarker(markerPath, spillWorkspace, baselineFileIdSet);
    } catch (final IOException error) {
      throw new IndexException("Cannot create sorted index build cleanup manifest " + markerPath, error);
    }
  }

  Path getSpillWorkspace() {
    return spillWorkspace;
  }

  boolean baselineRestored(final DatabaseInternal database) {
    for (final ComponentFile file : database.getFileManager().getFiles())
      if (file != null && !baselineFileIds.contains(file.getFileId()))
        return false;
    return true;
  }

  void clear() {
    try {
      removeRecursively(spillWorkspace);
      if (Files.deleteIfExists(markerPath))
        forceDirectory(markerPath.getParent());
    } catch (final IOException error) {
      throw new IndexException("Cannot remove sorted index build cleanup manifest " + markerPath, error);
    }
  }

  static void recoverInterruptedBuilds(final DatabaseInternal database, final ComponentFile.MODE mode) throws IOException {
    final Path databasePath = Path.of(database.getDatabasePath()).toAbsolutePath().normalize();
    final List<Path> markers;
    try (Stream<Path> files = Files.list(databasePath)) {
      markers = files.filter(Files::isRegularFile)
          .filter(path -> path.getFileName().toString().startsWith(FILE_PREFIX))
          .filter(path -> path.getFileName().toString().endsWith(FILE_SUFFIX))
          .sorted()
          .toList();
    }
    if (markers.isEmpty())
      return;
    if (mode != ComponentFile.MODE.READ_WRITE)
      throw new IOException("Database contains an interrupted sorted index build; reopen it read-write to perform cleanup");

    for (final Path markerPath : markers)
      recoverMarker(database, databasePath, markerPath);
    forceDirectory(databasePath);
  }

  private static void recoverMarker(final DatabaseInternal database, final Path databasePath, final Path markerPath)
      throws IOException {
    final JSONObject marker = new JSONObject(Files.readString(markerPath, StandardCharsets.UTF_8));
    if (marker.getInt("version") != 1)
      throw new IOException("Unsupported sorted index build cleanup manifest version in " + markerPath);

    final String typeName = marker.getString("typeName");
    final List<String> propertyNames = marker.getJSONArray("propertyNames").toListOfStrings();
    final boolean published = isPublished(databasePath, typeName, propertyNames);

    int removedFiles = 0;
    if (!published) {
      final Set<Integer> baselineFileIds = new HashSet<>();
      final JSONArray ids = marker.getJSONArray("baselineFileIds");
      for (int i = 0; i < ids.length(); i++)
        baselineFileIds.add(ids.getInt(i));

      final List<ComponentFile> currentFiles = new ArrayList<>(database.getFileManager().getFiles());
      for (final ComponentFile file : currentFiles)
        if (file != null && !baselineFileIds.contains(file.getFileId())) {
          LogManager.instance().log(database, Level.WARNING,
              "Removing component '%s' (fileId=%d) left by an interrupted sorted index build", null,
              file.getFileName(), file.getFileId());
          database.getFileManager().dropFile(file.getFileId());
          removedFiles++;
        }
    }

    final Path spillWorkspace = Path.of(marker.getString("spillWorkspace")).toAbsolutePath().normalize();
    validateSpillWorkspace(spillWorkspace, markerPath);
    removeRecursively(spillWorkspace);
    Files.deleteIfExists(markerPath);

    LogManager.instance().log(database, Level.WARNING,
        "Recovered interrupted sorted index build for %s%s (published=%s removedFiles=%d)", null, typeName,
        propertyNames, published, removedFiles);
  }

  private static boolean isPublished(final Path databasePath, final String typeName, final List<String> propertyNames) {
    final Path schemaPath = databasePath.resolve(LocalSchema.SCHEMA_FILE_NAME);
    if (!Files.isRegularFile(schemaPath))
      return false;

    try {
      final JSONObject root = new JSONObject(Files.readString(schemaPath, StandardCharsets.UTF_8));
      final JSONObject types = root.getJSONObject("types", null);
      final JSONObject type = types != null ? types.getJSONObject(typeName, null) : null;
      final JSONObject indexes = type != null ? type.getJSONObject("indexes", null) : null;
      if (indexes == null)
        return false;

      for (final String indexName : indexes.keySet()) {
        final JSONArray properties = indexes.getJSONObject(indexName).getJSONArray("properties");
        if (propertyNames.equals(properties.toListOfStrings()))
          return true;
      }
    } catch (final Exception ignore) {
      // A missing or corrupt current schema falls back to the previous schema during normal load.
    }
    return false;
  }

  private static void validateSpillWorkspace(final Path spillWorkspace, final Path markerPath) throws IOException {
    final Path fileName = spillWorkspace.getFileName();
    if (fileName == null || !fileName.toString().startsWith(SORT_DIRECTORY_PREFIX))
      throw new IOException("Invalid spill workspace in sorted index build cleanup manifest " + markerPath);
  }

  private static void removeRecursively(final Path root) throws IOException {
    if (!Files.exists(root))
      return;
    final List<Path> paths;
    try (Stream<Path> walk = Files.walk(root)) {
      paths = walk.sorted((left, right) -> right.compareTo(left)).toList();
    }
    for (final Path path : paths)
      Files.deleteIfExists(path);
  }

  private static void forceFile(final Path path) throws IOException {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.WRITE)) {
      channel.force(true);
    }
  }

  private static void forceDirectory(final Path path) {
    try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
      channel.force(true);
    } catch (final IOException | UnsupportedOperationException ignore) {
      // The manifest itself is forced; directory fsync is best effort where supported.
    }
  }
}
