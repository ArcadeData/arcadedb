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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.log.LogManager;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * Reads and writes a {@link GraphAnalyticalView}'s built CSR (adjacency indexes, node ID mapping and columnar
 * property storage) to a single file beside the database, so that a later open can reuse it instead of rebuilding
 * it with a full graph scan (see issue #6583).
 * <p>
 * The header carries a freshness certificate — {@code asOfTransactionId}, the database's last committed
 * transaction id sampled just before the scan that produced this CSR started (see {@link
 * GraphAnalyticalView.Snapshot#asOfTransactionId}) — plus the exact vertex/edge/property filter the view was built
 * with. {@link #load} checks both against the caller-supplied current state before it even attempts to parse the
 * (potentially large) payload that follows: a database-wide transaction count is a coarser signal than "did a
 * covered type change", but it is sound with no extra bookkeeping — if nothing at all was committed since the
 * certificate was written, the covered types certainly didn't change either — and it costs nothing to check, unlike
 * a per-type watermark this engine does not otherwise maintain.
 * <p>
 * Any failure to parse the file (corruption, truncation, a version this build does not understand, a checksum
 * mismatch) is treated exactly like a missing file: {@link #load} returns null and the caller falls back to a full
 * rebuild, exactly as it always has when nothing was persisted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewCSRPersistence {
  private static final int    MAGIC           = 0x47415643; // "GAVC"
  private static final int    FORMAT_VERSION  = 1;
  private static final String FILE_PREFIX     = "gav-";
  private static final String FILE_EXTENSION  = ".csr";

  private GraphAnalyticalViewCSRPersistence() {
  }

  static File fileFor(final Database database, final String viewName) {
    return new File(database.getDatabasePath(), FILE_PREFIX + viewName + FILE_EXTENSION);
  }

  static void delete(final Database database, final String viewName) {
    try {
      Files.deleteIfExists(fileFor(database, viewName).toPath());
    } catch (final IOException e) {
      LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.FINE,
          "Could not delete persisted CSR file for GraphAnalyticalView '%s': %s", null, viewName, e.getMessage());
    }
  }

  // --- Save ---

  static void save(final Database database, final String viewName, final String[] vertexTypes, final String[] edgeTypes,
      final String[] propertyFilter, final String[] edgePropertyFilter, final GraphAnalyticalView.Snapshot snapshot)
      throws IOException {
    final File target = fileFor(database, viewName);
    final File parent = target.getParentFile();
    if (parent != null && !parent.exists())
      Files.createDirectories(parent.toPath());
    final File tmp = new File(parent, target.getName() + "." + Long.toHexString(System.nanoTime()) + ".tmp");

    try (final FileOutputStream fos = new FileOutputStream(tmp);
        final CheckedOutputStream checked = new CheckedOutputStream(new BufferedOutputStream(fos), new CRC32());
        final DataOutputStream out = new DataOutputStream(checked)) {

      out.writeInt(MAGIC);
      out.writeInt(FORMAT_VERSION);
      writeStringArray(out, vertexTypes);
      writeStringArray(out, edgeTypes);
      writeStringArray(out, propertyFilter);
      writeStringArray(out, edgePropertyFilter);
      out.writeLong(snapshot.asOfTransactionId);

      writeNodeMapping(out, snapshot.nodeMapping);
      writeBucketColumns(out, snapshot.bucketColumns);
      writeCsrPerType(out, snapshot.csrPerType);
      writeOptionalColumnStoreMap(out, snapshot.edgeColumnStores);
      writeOptionalIntArrayMap(out, snapshot.bwdToFwd);

      out.flush();
      out.writeLong(checked.getChecksum().getValue());
    } catch (final IOException | RuntimeException e) {
      Files.deleteIfExists(tmp.toPath());
      throw e;
    }

    final Path targetPath = target.toPath();
    try {
      Files.move(tmp.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    } catch (final AtomicMoveNotSupportedException e) {
      Files.move(tmp.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  private static void writeStringArray(final DataOutputStream out, final String[] values) throws IOException {
    if (values == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(values.length);
    for (final String v : values)
      out.writeUTF(v);
  }

  private static void writeNodeMapping(final DataOutputStream out, final NodeIdMapping mapping) throws IOException {
    final int numBuckets = mapping.getNumBuckets();
    out.writeInt(numBuckets);
    for (int i = 0; i < numBuckets; i++) {
      out.writeInt(mapping.getBucketId(i));
      out.writeUTF(mapping.getBucketTypeName(i));
      final int size = mapping.getBucketSize(i);
      out.writeInt(size);
      for (int p = 0; p < size; p++)
        out.writeLong(mapping.getPosition(i, p));
    }
    final int[] oldToNew = mapping.getOldToNewMapping();
    out.writeBoolean(oldToNew != null);
    if (oldToNew != null)
      writeIntArray(out, oldToNew);
  }

  private static void writeBucketColumns(final DataOutputStream out, final ColumnStore[] bucketColumns) throws IOException {
    out.writeInt(bucketColumns.length);
    for (final ColumnStore store : bucketColumns)
      writeColumnStore(out, store);
  }

  private static void writeColumnStore(final DataOutputStream out, final ColumnStore store) throws IOException {
    out.writeInt(store.getNodeCount());
    out.writeInt(store.getColumnCount());
    for (final String name : store.getPropertyNames())
      writeColumn(out, store.getColumn(name));
  }

  private static void writeColumn(final DataOutputStream out, final Column column) throws IOException {
    out.writeUTF(column.getName());
    out.writeByte(column.getType().ordinal());
    writeLongArray(out, column.getNullBitset());
    switch (column.getType()) {
    case INT:
      writeIntArray(out, column.getIntData());
      break;
    case LONG:
      writeLongArray(out, column.getLongData());
      break;
    case DOUBLE:
      writeDoubleArray(out, column.getDoubleData());
      break;
    case STRING:
      writeStringArray(out, column.getDictionary().getValues());
      writeIntArray(out, column.getStringCodes());
      break;
    }
  }

  private static void writeCsrPerType(final DataOutputStream out, final Map<String, CSRAdjacencyIndex> csrPerType) throws IOException {
    out.writeInt(csrPerType.size());
    for (final Map.Entry<String, CSRAdjacencyIndex> entry : csrPerType.entrySet()) {
      out.writeUTF(entry.getKey());
      final CSRAdjacencyIndex csr = entry.getValue();
      out.writeInt(csr.getNodeCount());
      out.writeInt(csr.getEdgeCount());
      writeIntArray(out, csr.getForwardOffsets());
      writeIntArray(out, csr.getForwardNeighbors());
      writeIntArray(out, csr.getBackwardOffsets());
      writeIntArray(out, csr.getBackwardNeighbors());
    }
  }

  private static void writeOptionalColumnStoreMap(final DataOutputStream out, final Map<String, ColumnStore> edgeColumnStores)
      throws IOException {
    out.writeBoolean(edgeColumnStores != null);
    if (edgeColumnStores == null)
      return;
    out.writeInt(edgeColumnStores.size());
    for (final Map.Entry<String, ColumnStore> entry : edgeColumnStores.entrySet()) {
      out.writeUTF(entry.getKey());
      writeColumnStore(out, entry.getValue());
    }
  }

  private static void writeOptionalIntArrayMap(final DataOutputStream out, final Map<String, int[]> map) throws IOException {
    out.writeBoolean(map != null);
    if (map == null)
      return;
    out.writeInt(map.size());
    for (final Map.Entry<String, int[]> entry : map.entrySet()) {
      out.writeUTF(entry.getKey());
      writeIntArray(out, entry.getValue());
    }
  }

  private static void writeIntArray(final DataOutputStream out, final int[] array) throws IOException {
    out.writeInt(array.length);
    for (final int v : array)
      out.writeInt(v);
  }

  private static void writeLongArray(final DataOutputStream out, final long[] array) throws IOException {
    out.writeInt(array.length);
    for (final long v : array)
      out.writeLong(v);
  }

  private static void writeDoubleArray(final DataOutputStream out, final double[] array) throws IOException {
    out.writeInt(array.length);
    for (final double v : array)
      out.writeDouble(v);
  }

  // --- Load ---

  /**
   * Loads a persisted CSR, returning null (never throwing for anything short of an I/O error unrelated to the
   * file's own content) when the file is absent, its definition doesn't match the caller's, or its certificate
   * doesn't match {@code currentLastTransactionId}. In the last two cases the header is all that gets read.
   */
  static GraphAnalyticalView.Snapshot load(final Database database, final String viewName, final String[] vertexTypes,
      final String[] edgeTypes, final String[] propertyFilter, final String[] edgePropertyFilter,
      final long currentLastTransactionId) throws IOException {
    final File file = fileFor(database, viewName);
    if (!file.isFile())
      return null;

    try (final FileInputStream fis = new FileInputStream(file);
        final CheckedInputStream checked = new CheckedInputStream(new BufferedInputStream(fis), new CRC32());
        final DataInputStream in = new DataInputStream(checked)) {

      if (in.readInt() != MAGIC || in.readInt() != FORMAT_VERSION)
        return null;
      if (!Arrays.equals(readStringArray(in), vertexTypes)
          || !Arrays.equals(readStringArray(in), edgeTypes)
          || !Arrays.equals(readStringArray(in), propertyFilter)
          || !Arrays.equals(readStringArray(in), edgePropertyFilter))
        return null;
      final long asOfTransactionId = in.readLong();
      if (asOfTransactionId != currentLastTransactionId)
        return null;

      final NodeIdMapping mapping = readNodeMapping(in);
      final ColumnStore[] bucketColumns = readBucketColumns(in);
      final Map<String, CSRAdjacencyIndex> csrPerType = readCsrPerType(in);
      final Map<String, ColumnStore> edgeColumnStores = readOptionalColumnStoreMap(in);
      final Map<String, int[]> bwdToFwd = readOptionalIntArrayMap(in);

      final long computedCrc = checked.getChecksum().getValue();
      final long storedCrc = in.readLong();
      if (computedCrc != storedCrc) {
        LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
            "Persisted CSR for GraphAnalyticalView '%s' failed checksum verification, discarding and falling back to rebuild",
            null, viewName);
        delete(database, viewName);
        return null;
      }

      return new GraphAnalyticalView.Snapshot(csrPerType, mapping, bucketColumns, edgeColumnStores, bwdToFwd,
          null, System.currentTimeMillis(), 0L, asOfTransactionId, true);
    } catch (final EOFException | NegativeArraySizeException | IllegalArgumentException | ArrayIndexOutOfBoundsException e) {
      LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
          "Persisted CSR for GraphAnalyticalView '%s' is truncated or corrupt (%s), discarding and falling back to rebuild",
          null, viewName, e.toString());
      delete(database, viewName);
      return null;
    }
  }

  private static String[] readStringArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    if (len < 0)
      return null;
    final String[] values = new String[len];
    for (int i = 0; i < len; i++)
      values[i] = in.readUTF();
    return values;
  }

  private static NodeIdMapping readNodeMapping(final DataInputStream in) throws IOException {
    final int numBuckets = in.readInt();
    final int[] bucketIds = new int[numBuckets];
    final String[] bucketTypeNames = new String[numBuckets];
    final long[][] positions = new long[numBuckets][];
    for (int i = 0; i < numBuckets; i++) {
      bucketIds[i] = in.readInt();
      bucketTypeNames[i] = in.readUTF();
      final int size = in.readInt();
      positions[i] = readLongArray(in, size);
    }
    final boolean reordered = in.readBoolean();
    final int[] oldToNew = reordered ? readIntArray(in) : null;
    return NodeIdMapping.restore(bucketIds, bucketTypeNames, positions, oldToNew);
  }

  private static ColumnStore[] readBucketColumns(final DataInputStream in) throws IOException {
    final int numBuckets = in.readInt();
    final ColumnStore[] result = new ColumnStore[numBuckets];
    for (int i = 0; i < numBuckets; i++)
      result[i] = readColumnStore(in);
    return result;
  }

  private static ColumnStore readColumnStore(final DataInputStream in) throws IOException {
    final int nodeCount = in.readInt();
    final int columnCount = in.readInt();
    final ColumnStore store = new ColumnStore(nodeCount);
    for (int c = 0; c < columnCount; c++)
      store.putColumn(readColumn(in, nodeCount));
    return store;
  }

  private static Column readColumn(final DataInputStream in, final int nodeCount) throws IOException {
    final String name = in.readUTF();
    final Column.Type type = Column.Type.values()[in.readUnsignedByte()];
    final long[] nullBitset = readLongArray(in);
    int[] intData = null;
    long[] longData = null;
    double[] doubleData = null;
    int[] stringCodes = null;
    String[] dictionaryValues = null;
    switch (type) {
    case INT:
      intData = readIntArray(in);
      break;
    case LONG:
      longData = readLongArray(in);
      break;
    case DOUBLE:
      doubleData = readDoubleArray(in);
      break;
    case STRING:
      dictionaryValues = readStringArray(in);
      stringCodes = readIntArray(in);
      break;
    }
    return Column.restore(name, type, nodeCount, nullBitset, intData, longData, doubleData, stringCodes, dictionaryValues);
  }

  private static Map<String, CSRAdjacencyIndex> readCsrPerType(final DataInputStream in) throws IOException {
    final int count = in.readInt();
    final Map<String, CSRAdjacencyIndex> result = new HashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++) {
      final String edgeTypeName = in.readUTF();
      final int nodeCount = in.readInt();
      final int edgeCount = in.readInt();
      final int[] fwdOffsets = readIntArray(in);
      final int[] fwdNeighbors = readIntArray(in);
      final int[] bwdOffsets = readIntArray(in);
      final int[] bwdNeighbors = readIntArray(in);
      result.put(edgeTypeName, new CSRAdjacencyIndex(fwdOffsets, fwdNeighbors, bwdOffsets, bwdNeighbors, nodeCount, edgeCount));
    }
    return result;
  }

  private static Map<String, ColumnStore> readOptionalColumnStoreMap(final DataInputStream in) throws IOException {
    if (!in.readBoolean())
      return null;
    final int count = in.readInt();
    final Map<String, ColumnStore> result = new LinkedHashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++)
      result.put(in.readUTF(), readColumnStore(in));
    return result;
  }

  private static Map<String, int[]> readOptionalIntArrayMap(final DataInputStream in) throws IOException {
    if (!in.readBoolean())
      return null;
    final int count = in.readInt();
    final Map<String, int[]> result = new LinkedHashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++)
      result.put(in.readUTF(), readIntArray(in));
    return result;
  }

  private static int[] readIntArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    final int[] array = new int[len];
    for (int i = 0; i < len; i++)
      array[i] = in.readInt();
    return array;
  }

  private static long[] readLongArray(final DataInputStream in) throws IOException {
    return readLongArray(in, in.readInt());
  }

  private static long[] readLongArray(final DataInputStream in, final int len) throws IOException {
    final long[] array = new long[len];
    for (int i = 0; i < len; i++)
      array[i] = in.readLong();
    return array;
  }

  private static double[] readDoubleArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    final double[] array = new double[len];
    for (int i = 0; i < len; i++)
      array[i] = in.readDouble();
    return array;
  }
}
