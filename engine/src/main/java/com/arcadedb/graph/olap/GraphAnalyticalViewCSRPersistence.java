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

import com.arcadedb.database.DataEncryption;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.engine.NodeLocalFiles;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.IntHashSet;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.zip.CRC32;

/**
 * Reads and writes a {@link GraphAnalyticalView}'s built CSR (adjacency indexes, node ID mapping and columnar
 * property storage) to a single file beside the database, so that a later open can reuse it instead of rebuilding
 * it with a full graph scan (see issue #6583).
 * <p>
 * The file is a small plaintext header followed by one opaque payload blob:
 * <ul>
 *   <li>the header carries the freshness certificate - {@code asOfTransactionId}, the database's last committed
 *       transaction id sampled just before the scan that produced this CSR started (see {@link
 *       GraphAnalyticalView.Snapshot#asOfTransactionId}) - plus the exact vertex/edge/property filter the view was
 *       built with. {@link #load} checks both against the caller-supplied current state before it even attempts to
 *       touch the (potentially large) payload: a database-wide transaction count is a coarser signal than "did a
 *       covered type change", but it is sound with no extra bookkeeping - if nothing at all was committed since the
 *       certificate was written, the covered types certainly didn't change either - and it costs nothing to check,
 *       unlike a per-type watermark this engine does not otherwise maintain;</li>
 *   <li>the payload holds every array making up the CSR, built once into an in-memory buffer and written (or read)
 *       in one bulk I/O call per array via {@link ByteBuffer} bulk views rather than one JDK stream call per array
 *       element - this is a graph's serialized size, so it is exactly the part a per-element loop would make slow at
 *       the scale this issue is about. When the database has a {@link DataEncryption} configured (see {@link
 *       DatabaseInternal#getDataEncryption()}), the assembled payload is encrypted as a single block before it
 *       reaches disk and decrypted as a single block after being read back, so opting into encryption for a
 *       database's records also covers this side file: the header itself carries no vertex/edge/property values,
 *       only type and property *names* already visible in the schema, so it is left in the clear.</li>
 * </ul>
 * <p>
 * Any failure to parse the file (corruption, truncation, a version this build does not understand, a checksum
 * mismatch) is treated exactly like a missing file: {@link #load} returns null and the caller falls back to a full
 * rebuild, exactly as it always has when nothing was persisted.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewCSRPersistence {
  private static final int    MAGIC          = 0x47415643; // "GAVC"
  // 3: a listed vertex type brings its sub-types (issue #10208), so a file written for the type alone is not reused
  // 4: the header carries the scan's coverage of the edge types (issue #10214)
  // 5: the payload is a sequence of frames, each encrypted on its own, so neither side holds it whole (issue #11011)
  private static final int    FORMAT_VERSION   = 5;
  private static final int    FRAME_SIZE       = 1 << 20;
  private static final int    ARRAY_CHUNK_SIZE = 1 << 16;
  private static final int    IO_BUFFER_SIZE   = 1 << 16;
  // NODE-LOCAL (#9638): DECLARED IN NodeLocalFiles SO A CROSS-NODE CHECKSUM LEAVES THE FILE OUT
  private static final String FILE_PREFIX    = NodeLocalFiles.GAV_CSR_FILE_PREFIX;
  private static final String FILE_EXTENSION = NodeLocalFiles.GAV_CSR_FILE_EXTENSION;

  private GraphAnalyticalViewCSRPersistence() {
  }

  /**
   * A GAV name is a SQL identifier a schema-privileged user controls, and (unlike a type or bucket name) nothing
   * upstream of this class validates it against path separators - a name such as {@code ../../../tmp/evil} would
   * otherwise let {@link #save}/{@link #delete} write, overwrite or delete an arbitrary file the server process can
   * reach. Encode it exactly like {@link com.arcadedb.schema.LocalSchema} encodes a type name before it becomes a
   * component file name: {@link FileUtils#encode} percent-encodes {@code /} and {@code \} (and leaves the rest of
   * the identifier's characters intact), so the result is always a single path segment under the database directory
   * regardless of what the name contains.
   */
  static File fileFor(final Database database, final String viewName) {
    final String encodedName = FileUtils.encode(viewName, database.getSchema().getEncoding());
    return new File(database.getDatabasePath(), FILE_PREFIX + encodedName + FILE_EXTENSION);
  }

  static void delete(final Database database, final String viewName) {
    try {
      Files.deleteIfExists(fileFor(database, viewName).toPath());
    } catch (final IOException e) {
      LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.FINE,
          "Could not delete persisted CSR file for GraphAnalyticalView '%s': %s", null, viewName, e.getMessage());
    }
  }

  private static DataEncryption dataEncryptionOf(final Database database) {
    return ((DatabaseInternal) database).getDataEncryption();
  }

  // --- Save ---

  /**
   * @param scannedEdgeTypes     the edge types the scan materialized, null when not known
   * @param uncoveredEdgeBuckets the buckets of the edges of a type the view does not materialize that the scan met (issue
   *                             #10214); null when not known. An edge written after the scan needs no record here: its commit
   *                             moves the transaction id past the certificate, and the file is not restored
   */
  static void save(final Database database, final String viewName, final String[] vertexTypes, final String[] edgeTypes,
      final String[] propertyFilter, final String[] edgePropertyFilter, final GraphAnalyticalView.Snapshot snapshot,
      final String[] scannedEdgeTypes, final int[] uncoveredEdgeBuckets) throws IOException {
    final DataEncryption encryption = dataEncryptionOf(database);

    final File target = fileFor(database, viewName);
    final File parent = target.getParentFile();
    if (parent != null && !parent.exists())
      Files.createDirectories(parent.toPath());
    final File tmp = new File(parent, target.getName() + "." + Long.toHexString(System.nanoTime()) + ".tmp");

    try (final DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmp), IO_BUFFER_SIZE))) {
      out.writeInt(MAGIC);
      out.writeInt(FORMAT_VERSION);
      writeString(out, viewName);
      writeStringArray(out, vertexTypes);
      writeStringArray(out, edgeTypes);
      writeStringArray(out, propertyFilter);
      writeStringArray(out, edgePropertyFilter);
      writeStringArray(out, scannedEdgeTypes);
      writeOptionalInts(out, uncoveredEdgeBuckets);
      out.writeLong(snapshot.asOfTransactionId);
      out.writeBoolean(encryption != null);

      // The payload is streamed to the file in frames rather than assembled in memory first: a buffer of the whole payload
      // held the view a second time, and its growth and the copy out of it a third, while the view was being written
      // (issue #11011)
      final CRC32 crc = new CRC32();
      final FramedOutputStream frames = new FramedOutputStream(out, crc, encryption);
      final DataOutputStream payloadOut = new DataOutputStream(frames);
      writeNodeMapping(payloadOut, snapshot.nodeMapping);
      writeBucketColumns(payloadOut, snapshot.bucketColumns);
      writeCsrPerType(payloadOut, snapshot.csrPerType);
      writeOptionalColumnStoreMap(payloadOut, snapshot.edgeColumnStores);
      writeOptionalIntArrayMap(payloadOut, snapshot.bwdToFwd);
      payloadOut.flush();
      frames.finish();
      out.writeLong(crc.getValue());
    } catch (final IOException | RuntimeException e) {
      Files.deleteIfExists(tmp.toPath());
      throw e;
    }

    final Path targetPath = target.toPath();
    try {
      try {
        Files.move(tmp.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (final AtomicMoveNotSupportedException e) {
        Files.move(tmp.toPath(), targetPath, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (final IOException e) {
      // A failing move (disk full, permissions) would otherwise leave the tmp file - nanoTime()-suffixed, so a
      // persistently failing move orphans a new one on every close - behind forever alongside a still-usable
      // (if stale) previous target file. Clean it up either way; the caller already treats a save() failure as
      // "no persistence this cycle", exactly as if this file had never been written.
      Files.deleteIfExists(tmp.toPath());
      throw e;
    }
  }

  private static void writeString(final DataOutputStream out, final String value) throws IOException {
    if (value == null) {
      out.writeInt(-1);
      return;
    }
    final byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
  }

  private static void writeStringArray(final DataOutputStream out, final String[] values) throws IOException {
    if (values == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(values.length);
    for (final String v : values)
      writeString(out, v);
  }

  private static void writeOptionalInts(final DataOutputStream out, final int[] values) throws IOException {
    if (values == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(values.length);
    for (final int v : values)
      out.writeInt(v);
  }

  private static int[] readOptionalInts(final BoundedInput in) throws IOException {
    // READ BEFORE THE CHECKSUM: A CORRUPT HEADER MUST NOT SIZE AN ALLOCATION. ONE ENTRY PER BUCKET, SO THE FILE BOUNDS IT
    final int length = in.readLength(Integer.BYTES);
    if (length < 0)
      return null;
    final int[] values = new int[length];
    for (int i = 0; i < length; i++)
      values[i] = in.readInt();
    return values;
  }

  private static void writeNodeMapping(final DataOutputStream out, final NodeIdMapping mapping) throws IOException {
    final int numBuckets = mapping.getNumBuckets();
    out.writeInt(numBuckets);
    for (int i = 0; i < numBuckets; i++) {
      out.writeInt(mapping.getBucketId(i));
      writeString(out, mapping.getBucketTypeName(i));
      writeLongArray(out, mapping.getPositions(i));
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
    writeString(out, column.getName());
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
      writeString(out, entry.getKey());
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
      writeString(out, entry.getKey());
      writeColumnStore(out, entry.getValue());
    }
  }

  private static void writeOptionalIntArrayMap(final DataOutputStream out, final Map<String, int[]> map) throws IOException {
    out.writeBoolean(map != null);
    if (map == null)
      return;
    out.writeInt(map.size());
    for (final Map.Entry<String, int[]> entry : map.entrySet()) {
      writeString(out, entry.getKey());
      writeIntArray(out, entry.getValue());
    }
  }

  /**
   * Writes the array length followed by its raw bytes in bulk {@link ByteBuffer} conversions of {@link #ARRAY_CHUNK_SIZE}
   * bytes each, instead of one {@code DataOutputStream} call per element - the difference between a handful of bulk
   * copies and tens of millions of small virtual calls for the multi-million-element offset/neighbor arrays a 1M-vertex
   * CSR produces - and without a byte copy of the whole array, which for a neighbour array is the size of the slice.
   */
  private static void writeIntArray(final DataOutputStream out, final int[] array) throws IOException {
    out.writeInt(array.length);
    final byte[] chunk = new byte[(int) Math.min(ARRAY_CHUNK_SIZE, (long) array.length * Integer.BYTES)];
    final int perChunk = ARRAY_CHUNK_SIZE / Integer.BYTES;
    for (int from = 0; from < array.length; from += perChunk) {
      final int count = Math.min(perChunk, array.length - from);
      ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(array, from, count);
      out.write(chunk, 0, count * Integer.BYTES);
    }
  }

  private static void writeLongArray(final DataOutputStream out, final long[] array) throws IOException {
    out.writeInt(array.length);
    final byte[] chunk = new byte[(int) Math.min(ARRAY_CHUNK_SIZE, (long) array.length * Long.BYTES)];
    final int perChunk = ARRAY_CHUNK_SIZE / Long.BYTES;
    for (int from = 0; from < array.length; from += perChunk) {
      final int count = Math.min(perChunk, array.length - from);
      ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN).asLongBuffer().put(array, from, count);
      out.write(chunk, 0, count * Long.BYTES);
    }
  }

  private static void writeDoubleArray(final DataOutputStream out, final double[] array) throws IOException {
    out.writeInt(array.length);
    final byte[] chunk = new byte[(int) Math.min(ARRAY_CHUNK_SIZE, (long) array.length * Double.BYTES)];
    final int perChunk = ARRAY_CHUNK_SIZE / Double.BYTES;
    for (int from = 0; from < array.length; from += perChunk) {
      final int count = Math.min(perChunk, array.length - from);
      ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN).asDoubleBuffer().put(array, from, count);
      out.write(chunk, 0, count * Double.BYTES);
    }
  }

  // --- Load ---

  /**
   * Loads a persisted CSR, returning null (never throwing for anything short of an I/O error unrelated to the
   * file's own content) when the file is absent, its definition doesn't match the caller's, or its certificate
   * doesn't match {@code currentLastTransactionId}. In the last two cases the payload is never even read.
   */
  static GraphAnalyticalView.Snapshot load(final Database database, final String viewName, final String[] vertexTypes,
      final String[] edgeTypes, final String[] propertyFilter, final String[] edgePropertyFilter,
      final long currentLastTransactionId, final long uncoveredWritesMark) throws IOException {
    final File file = fileFor(database, viewName);
    if (!file.isFile())
      return null;

    // Every length the file declares is checked against what the file can hold before it sizes an allocation: the payload
    // is parsed as it is read, so its checksum is known only once the arrays are built
    try (final BoundedInput in = new BoundedInput(new BufferedInputStream(new FileInputStream(file), IO_BUFFER_SIZE), file.length())) {
      if (in.readInt() != MAGIC || in.readInt() != FORMAT_VERSION)
        return null;
      if (!viewName.equals(readString(in))
          || !Arrays.equals(readStringArray(in), vertexTypes)
          || !Arrays.equals(readStringArray(in), edgeTypes)
          || !Arrays.equals(readStringArray(in), propertyFilter)
          || !Arrays.equals(readStringArray(in), edgePropertyFilter))
        return null;
      final String[] scannedEdgeTypes = readStringArray(in);
      final int[] uncoveredEdgeBuckets = readOptionalInts(in);
      final long asOfTransactionId = in.readLong();
      if (asOfTransactionId != currentLastTransactionId)
        return null;
      final boolean encrypted = in.readBoolean();
      final DataEncryption encryption = dataEncryptionOf(database);
      if (encrypted && encryption == null)
        throw new IOException("persisted CSR is encrypted but no DataEncryption is configured on this database");

      // The payload is parsed while it streams in, frame by frame, so the view is held once while it is read back, not
      // once as the bytes read and again as the arrays built from them (issue #11011)
      final CRC32 crc = new CRC32();
      final FramedInputStream frames = new FramedInputStream(in, crc, encrypted ? encryption : null);
      final BoundedInput payloadIn = new BoundedInput(frames, file.length());
      final NodeIdMapping mapping = readNodeMapping(payloadIn);
      final ColumnStore[] bucketColumns = readBucketColumns(payloadIn);
      final Map<String, CSRAdjacencyIndex> csrPerType = readCsrPerType(payloadIn);
      final Map<String, ColumnStore> edgeColumnStores = readOptionalColumnStoreMap(payloadIn);
      final Map<String, int[]> bwdToFwd = readOptionalIntArrayMap(payloadIn);
      frames.finish();

      final long storedCrc = in.readLong();
      if (crc.getValue() != storedCrc) {
        LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
            "Persisted CSR for GraphAnalyticalView '%s' failed checksum verification, discarding and falling back to rebuild",
            null, viewName);
        delete(database, viewName);
        return null;
      }

      // Nothing was committed since the file was written (the certificate above), so the scan's coverage still holds, measured
      // from the restoring view's own counter of the uncovered edges written
      IntHashSet uncovered = null;
      if (uncoveredEdgeBuckets != null) {
        uncovered = new IntHashSet(Math.max(16, uncoveredEdgeBuckets.length * 2));
        for (final int bucketId : uncoveredEdgeBuckets)
          uncovered.add(bucketId);
      }
      return new GraphAnalyticalView.Snapshot(csrPerType, mapping, bucketColumns, edgeColumnStores, bwdToFwd,
          null, System.currentTimeMillis(), 0L, asOfTransactionId, true,
          vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter, -1L,
          scannedEdgeTypes != null ? Set.copyOf(Arrays.asList(scannedEdgeTypes)) : null, uncovered != null ? uncoveredWritesMark : -1L, uncovered);
    } catch (final IOException | OutOfMemoryError | RuntimeException e) {
      // Truncation, corruption, a payload that does not decrypt, and the "encrypted but no DataEncryption configured" case
      // thrown above all take the same log+delete+return-null path, rather than propagating past this method to be caught
      // (still safely, just less informatively) by the broad Exception handling in tryRestoreFromPersistedCsr()
      LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
          "Persisted CSR for GraphAnalyticalView '%s' is truncated, corrupt or unusable (%s), discarding and falling back to rebuild",
          null, viewName, e.toString());
      delete(database, viewName);
      return null;
    }
  }

  private static String readString(final BoundedInput in) throws IOException {
    final int len = in.readLength(1);
    if (len < 0)
      return null;
    final byte[] bytes = new byte[len];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static String[] readStringArray(final BoundedInput in) throws IOException {
    // Each entry takes at least the four bytes of its own length
    final int len = in.readLength(Integer.BYTES);
    if (len < 0)
      return null;
    final String[] values = new String[len];
    for (int i = 0; i < len; i++)
      values[i] = readString(in);
    return values;
  }

  private static NodeIdMapping readNodeMapping(final BoundedInput in) throws IOException {
    final int numBuckets = in.readLength(Integer.BYTES);
    final int[] bucketIds = new int[numBuckets];
    final String[] bucketTypeNames = new String[numBuckets];
    final long[][] positions = new long[numBuckets][];
    for (int i = 0; i < numBuckets; i++) {
      bucketIds[i] = in.readInt();
      bucketTypeNames[i] = readString(in);
      positions[i] = readLongArray(in);
    }
    final boolean reordered = in.readBoolean();
    final int[] oldToNew = reordered ? readIntArray(in) : null;
    return NodeIdMapping.restore(bucketIds, bucketTypeNames, positions, oldToNew);
  }

  private static ColumnStore[] readBucketColumns(final BoundedInput in) throws IOException {
    final int numBuckets = in.readLength(Integer.BYTES);
    final ColumnStore[] result = new ColumnStore[numBuckets];
    for (int i = 0; i < numBuckets; i++)
      result[i] = readColumnStore(in);
    return result;
  }

  private static ColumnStore readColumnStore(final BoundedInput in) throws IOException {
    final int nodeCount = in.readInt();
    if (nodeCount < 0)
      throw new EOFException("negative node count " + nodeCount + " in the persisted CSR");
    final int columnCount = in.readLength(Integer.BYTES);
    final ColumnStore store = new ColumnStore(nodeCount);
    for (int c = 0; c < columnCount; c++)
      store.putColumn(readColumn(in, nodeCount));
    return store;
  }

  private static Column readColumn(final BoundedInput in, final int nodeCount) throws IOException {
    final String name = readString(in);
    final Column.Type type = Column.Type.values()[in.readUnsignedByte()];
    final long[] nullBitset = readLongArray(in);
    // The column is sized by the node count before the checksum is known: its null bitset, bounded by the file, must cover it
    if (nullBitset.length < ((nodeCount + 63L) >>> 6))
      throw new EOFException("node count " + nodeCount + " exceeds the null bitset of column '" + name + "' in the persisted CSR");
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

  private static Map<String, CSRAdjacencyIndex> readCsrPerType(final BoundedInput in) throws IOException {
    final int count = in.readLength(Integer.BYTES);
    final Map<String, CSRAdjacencyIndex> result = new HashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++) {
      final String edgeTypeName = readString(in);
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

  private static Map<String, ColumnStore> readOptionalColumnStoreMap(final BoundedInput in) throws IOException {
    if (!in.readBoolean())
      return null;
    final int count = in.readLength(Integer.BYTES);
    final Map<String, ColumnStore> result = new LinkedHashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++)
      result.put(readString(in), readColumnStore(in));
    return result;
  }

  private static Map<String, int[]> readOptionalIntArrayMap(final BoundedInput in) throws IOException {
    if (!in.readBoolean())
      return null;
    final int count = in.readLength(Integer.BYTES);
    final Map<String, int[]> result = new LinkedHashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++)
      result.put(readString(in), readIntArray(in));
    return result;
  }

  /**
   * Reads the length written by {@link #writeIntArray} followed by bulk reads and bulk {@link ByteBuffer} conversions of
   * {@link #ARRAY_CHUNK_SIZE} bytes each, mirroring the write side: the array is the only full-size allocation.
   */
  private static int[] readIntArray(final BoundedInput in) throws IOException {
    final int[] array = new int[in.readLength(Integer.BYTES)];
    final byte[] chunk = new byte[(int) Math.min(ARRAY_CHUNK_SIZE, (long) array.length * Integer.BYTES)];
    final int perChunk = ARRAY_CHUNK_SIZE / Integer.BYTES;
    for (int from = 0; from < array.length; from += perChunk) {
      final int count = Math.min(perChunk, array.length - from);
      in.readFully(chunk, 0, count * Integer.BYTES);
      ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN).asIntBuffer().get(array, from, count);
    }
    return array;
  }

  private static long[] readLongArray(final BoundedInput in) throws IOException {
    final long[] array = new long[in.readLength(Long.BYTES)];
    final byte[] chunk = new byte[(int) Math.min(ARRAY_CHUNK_SIZE, (long) array.length * Long.BYTES)];
    final int perChunk = ARRAY_CHUNK_SIZE / Long.BYTES;
    for (int from = 0; from < array.length; from += perChunk) {
      final int count = Math.min(perChunk, array.length - from);
      in.readFully(chunk, 0, count * Long.BYTES);
      ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN).asLongBuffer().get(array, from, count);
    }
    return array;
  }

  private static double[] readDoubleArray(final BoundedInput in) throws IOException {
    final double[] array = new double[in.readLength(Double.BYTES)];
    final byte[] chunk = new byte[(int) Math.min(ARRAY_CHUNK_SIZE, (long) array.length * Double.BYTES)];
    final int perChunk = ARRAY_CHUNK_SIZE / Double.BYTES;
    for (int from = 0; from < array.length; from += perChunk) {
      final int count = Math.min(perChunk, array.length - from);
      in.readFully(chunk, 0, count * Double.BYTES);
      ByteBuffer.wrap(chunk).order(ByteOrder.BIG_ENDIAN).asDoubleBuffer().get(array, from, count);
    }
    return array;
  }

  /**
   * A {@link DataInputStream} that checks every length the file declares against the bytes the file can hold before the
   * caller sizes an allocation with it. A payload is parsed as it streams in, before its checksum is known, so a corrupt
   * length must be refused on sight rather than reach the allocator: it would otherwise ask for up to 16 GB.
   */
  private static final class BoundedInput extends DataInputStream {
    private long unclaimed;

    private BoundedInput(final InputStream in, final long bound) {
      super(in);
      this.unclaimed = bound;
    }

    /**
     * Reads a length of entries of at least {@code entryBytes} bytes each, refusing one the file cannot hold. A negative
     * length is returned as is: it is how an absent value is written.
     */
    int readLength(final int entryBytes) throws IOException {
      final int length = readInt();
      if (length < 0)
        return length;
      final long bytes = (long) length * entryBytes;
      if (bytes > unclaimed)
        throw new EOFException("declared " + length + " entries exceed what the file holds");
      unclaimed -= bytes;
      return length;
    }
  }

  /**
   * Writes the payload as a sequence of frames of at most {@link #FRAME_SIZE} bytes, each prefixed with its stored length
   * and encrypted on its own when the database encrypts, followed by a {@code -1} end marker. Every byte written goes
   * through {@code crc}. Only one frame is held in memory, whatever the payload's size (issue #11011).
   */
  private static final class FramedOutputStream extends OutputStream {
    private final DataOutputStream out;
    private final CRC32            crc;
    private final DataEncryption   encryption;
    private final byte[]           frame = new byte[FRAME_SIZE];
    private       int              fill;

    private FramedOutputStream(final DataOutputStream out, final CRC32 crc, final DataEncryption encryption) {
      this.out = out;
      this.crc = crc;
      this.encryption = encryption;
    }

    @Override
    public void write(final int b) throws IOException {
      if (fill == frame.length)
        writeFrame();
      frame[fill++] = (byte) b;
    }

    @Override
    public void write(final byte[] bytes, int offset, int length) throws IOException {
      while (length > 0) {
        if (fill == frame.length)
          writeFrame();
        final int count = Math.min(length, frame.length - fill);
        System.arraycopy(bytes, offset, frame, fill, count);
        fill += count;
        offset += count;
        length -= count;
      }
    }

    /** Writes the last frame and the end marker. */
    void finish() throws IOException {
      writeFrame();
      writeLength(-1);
    }

    private void writeFrame() throws IOException {
      if (fill == 0)
        return;
      final byte[] stored = encryption != null ? encryption.encrypt(Arrays.copyOf(frame, fill)) : frame;
      final int length = encryption != null ? stored.length : fill;
      writeLength(length);
      out.write(stored, 0, length);
      crc.update(stored, 0, length);
      fill = 0;
    }

    private void writeLength(final int length) throws IOException {
      out.writeInt(length);
      updateWithInt(crc, length);
    }
  }

  /** Reads what {@link FramedOutputStream} wrote, one frame in memory at a time, every byte through {@code crc}. */
  private static final class FramedInputStream extends InputStream {
    private final DataInputStream in;
    private final CRC32           crc;
    private final DataEncryption  encryption;
    private       byte[]          frame = new byte[0];
    private       int             position;
    private       int             limit;
    private       boolean         ended;

    private FramedInputStream(final DataInputStream in, final CRC32 crc, final DataEncryption encryption) {
      this.in = in;
      this.crc = crc;
      this.encryption = encryption;
    }

    @Override
    public int read() throws IOException {
      while (position == limit)
        if (!nextFrame())
          return -1;
      return frame[position++] & 0xFF;
    }

    @Override
    public int read(final byte[] bytes, final int offset, final int length) throws IOException {
      if (length == 0)
        return 0;
      while (position == limit)
        if (!nextFrame())
          return -1;
      final int count = Math.min(length, limit - position);
      System.arraycopy(frame, position, bytes, offset, count);
      position += count;
      return count;
    }

    /** Checks that the payload ends where its content does: a frame left over means the file is not what was written. */
    void finish() throws IOException {
      if (position != limit || nextFrame())
        throw new EOFException("the persisted CSR holds more than its content");
    }

    private boolean nextFrame() throws IOException {
      if (ended)
        return false;
      final int length = in.readInt();
      updateWithInt(crc, length);
      if (length < 0) {
        ended = true;
        return false;
      }
      // An encrypted frame is its plain frame plus the cipher's own overhead, never twice its size
      if (length == 0 || length > 2 * FRAME_SIZE)
        throw new EOFException("invalid frame length " + length + " in the persisted CSR");
      // Without encryption the frame buffer is read into again: safe, since a frame is read only once the previous one is
      // consumed (position == limit), and nothing keeps a reference to its bytes
      final byte[] stored = encryption == null && frame.length >= length ? frame : new byte[length];
      in.readFully(stored, 0, length);
      crc.update(stored, 0, length);
      if (encryption != null) {
        frame = encryption.decrypt(stored);
        limit = frame.length;
      } else {
        frame = stored;
        limit = length;
      }
      position = 0;
      return true;
    }
  }

  /** Feeds the four big-endian bytes of {@code value} to {@code crc}, as {@link DataOutputStream#writeInt} writes them. */
  private static void updateWithInt(final CRC32 crc, final int value) {
    crc.update(value >>> 24);
    crc.update(value >>> 16);
    crc.update(value >>> 8);
    crc.update(value);
  }
}
