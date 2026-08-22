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
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
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
  private static final int    FORMAT_VERSION = 1;
  private static final String FILE_PREFIX    = "gav-";
  private static final String FILE_EXTENSION = ".csr";

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

  static void save(final Database database, final String viewName, final String[] vertexTypes, final String[] edgeTypes,
      final String[] propertyFilter, final String[] edgePropertyFilter, final GraphAnalyticalView.Snapshot snapshot)
      throws IOException {
    final ByteArrayOutputStream payloadBuffer = new ByteArrayOutputStream(1024);
    try (final DataOutputStream payloadOut = new DataOutputStream(payloadBuffer)) {
      writeNodeMapping(payloadOut, snapshot.nodeMapping);
      writeBucketColumns(payloadOut, snapshot.bucketColumns);
      writeCsrPerType(payloadOut, snapshot.csrPerType);
      writeOptionalColumnStoreMap(payloadOut, snapshot.edgeColumnStores);
      writeOptionalIntArrayMap(payloadOut, snapshot.bwdToFwd);
    }
    final byte[] rawPayload = payloadBuffer.toByteArray();

    final DataEncryption encryption = dataEncryptionOf(database);
    final byte[] storedPayload = encryption != null ? encryption.encrypt(rawPayload) : rawPayload;

    final CRC32 crc = new CRC32();
    crc.update(storedPayload);

    final File target = fileFor(database, viewName);
    final File parent = target.getParentFile();
    if (parent != null && !parent.exists())
      Files.createDirectories(parent.toPath());
    final File tmp = new File(parent, target.getName() + "." + Long.toHexString(System.nanoTime()) + ".tmp");

    try (final DataOutputStream out = new DataOutputStream(new FileOutputStream(tmp))) {
      out.writeInt(MAGIC);
      out.writeInt(FORMAT_VERSION);
      writeString(out, viewName);
      writeStringArray(out, vertexTypes);
      writeStringArray(out, edgeTypes);
      writeStringArray(out, propertyFilter);
      writeStringArray(out, edgePropertyFilter);
      out.writeLong(snapshot.asOfTransactionId);
      out.writeBoolean(encryption != null);
      out.writeInt(storedPayload.length);
      out.write(storedPayload);
      out.writeLong(crc.getValue());
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

  private static void writeNodeMapping(final DataOutputStream out, final NodeIdMapping mapping) throws IOException {
    final int numBuckets = mapping.getNumBuckets();
    out.writeInt(numBuckets);
    for (int i = 0; i < numBuckets; i++) {
      out.writeInt(mapping.getBucketId(i));
      writeString(out, mapping.getBucketTypeName(i));
      final int size = mapping.getBucketSize(i);
      final long[] positions = new long[size];
      for (int p = 0; p < size; p++)
        positions[p] = mapping.getPosition(i, p);
      writeLongArray(out, positions);
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
   * Writes the array length followed by its raw bytes in one bulk {@link ByteBuffer} conversion and one {@code
   * write(byte[])} call, instead of one {@code DataOutputStream} call per element - the difference between a handful
   * of bulk copies and tens of millions of small virtual calls for the multi-million-element offset/neighbor arrays
   * a 1M-vertex CSR produces.
   */
  private static void writeIntArray(final DataOutputStream out, final int[] array) throws IOException {
    out.writeInt(array.length);
    if (array.length == 0)
      return;
    final byte[] bytes = new byte[array.length * Integer.BYTES];
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer().put(array);
    out.write(bytes);
  }

  private static void writeLongArray(final DataOutputStream out, final long[] array) throws IOException {
    out.writeInt(array.length);
    if (array.length == 0)
      return;
    final byte[] bytes = new byte[array.length * Long.BYTES];
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asLongBuffer().put(array);
    out.write(bytes);
  }

  private static void writeDoubleArray(final DataOutputStream out, final double[] array) throws IOException {
    out.writeInt(array.length);
    if (array.length == 0)
      return;
    final byte[] bytes = new byte[array.length * Double.BYTES];
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asDoubleBuffer().put(array);
    out.write(bytes);
  }

  // --- Load ---

  /**
   * Loads a persisted CSR, returning null (never throwing for anything short of an I/O error unrelated to the
   * file's own content) when the file is absent, its definition doesn't match the caller's, or its certificate
   * doesn't match {@code currentLastTransactionId}. In the last two cases the payload is never even read.
   */
  static GraphAnalyticalView.Snapshot load(final Database database, final String viewName, final String[] vertexTypes,
      final String[] edgeTypes, final String[] propertyFilter, final String[] edgePropertyFilter,
      final long currentLastTransactionId) throws IOException {
    final File file = fileFor(database, viewName);
    if (!file.isFile())
      return null;

    final byte[] storedPayload;
    final boolean encrypted;
    final long asOfTransactionId;

    try (final DataInputStream in = new DataInputStream(new FileInputStream(file))) {
      if (in.readInt() != MAGIC || in.readInt() != FORMAT_VERSION)
        return null;
      if (!viewName.equals(readString(in))
          || !Arrays.equals(readStringArray(in), vertexTypes)
          || !Arrays.equals(readStringArray(in), edgeTypes)
          || !Arrays.equals(readStringArray(in), propertyFilter)
          || !Arrays.equals(readStringArray(in), edgePropertyFilter))
        return null;
      asOfTransactionId = in.readLong();
      if (asOfTransactionId != currentLastTransactionId)
        return null;
      encrypted = in.readBoolean();
      final int payloadLength = in.readInt();
      // A corrupt header could carry a huge but individually plausible length; check it against what's actually
      // left in the file before allocating, rather than relying on readFully() to eventually fail with
      // EOFException after the allocation already happened.
      if (payloadLength < 0 || payloadLength > file.length())
        throw new EOFException("declared payload length " + payloadLength + " exceeds the file's remaining size");
      storedPayload = new byte[payloadLength];
      in.readFully(storedPayload);

      final CRC32 crc = new CRC32();
      crc.update(storedPayload);
      final long computedCrc = crc.getValue();
      final long storedCrc = in.readLong();
      if (computedCrc != storedCrc) {
        LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
            "Persisted CSR for GraphAnalyticalView '%s' failed checksum verification, discarding and falling back to rebuild",
            null, viewName);
        delete(database, viewName);
        return null;
      }
    } catch (final EOFException | NegativeArraySizeException | OutOfMemoryError e) {
      LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
          "Persisted CSR for GraphAnalyticalView '%s' is truncated or corrupt (%s), discarding and falling back to rebuild",
          null, viewName, e.toString());
      delete(database, viewName);
      return null;
    }

    try {
      final DataEncryption encryption = dataEncryptionOf(database);
      if (encrypted && encryption == null)
        throw new IOException("persisted CSR is encrypted but no DataEncryption is configured on this database");
      final byte[] rawPayload = encrypted ? encryption.decrypt(storedPayload) : storedPayload;

      try (final DataInputStream payloadIn = new DataInputStream(new ByteArrayInputStream(rawPayload))) {
        final NodeIdMapping mapping = readNodeMapping(payloadIn);
        final ColumnStore[] bucketColumns = readBucketColumns(payloadIn);
        final Map<String, CSRAdjacencyIndex> csrPerType = readCsrPerType(payloadIn);
        final Map<String, ColumnStore> edgeColumnStores = readOptionalColumnStoreMap(payloadIn);
        final Map<String, int[]> bwdToFwd = readOptionalIntArrayMap(payloadIn);

        return new GraphAnalyticalView.Snapshot(csrPerType, mapping, bucketColumns, edgeColumnStores, bwdToFwd,
            null, System.currentTimeMillis(), 0L, asOfTransactionId, true,
            vertexTypes, edgeTypes, propertyFilter, edgePropertyFilter);
      }
    } catch (final EOFException | OutOfMemoryError | RuntimeException e) {
      LogManager.instance().log(GraphAnalyticalViewCSRPersistence.class, Level.WARNING,
          "Persisted CSR for GraphAnalyticalView '%s' is truncated or corrupt (%s), discarding and falling back to rebuild",
          null, viewName, e.toString());
      delete(database, viewName);
      return null;
    }
  }

  private static String readString(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    if (len < 0)
      return null;
    final byte[] bytes = new byte[len];
    in.readFully(bytes);
    return new String(bytes, StandardCharsets.UTF_8);
  }

  private static String[] readStringArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    if (len < 0)
      return null;
    final String[] values = new String[len];
    for (int i = 0; i < len; i++)
      values[i] = readString(in);
    return values;
  }

  private static NodeIdMapping readNodeMapping(final DataInputStream in) throws IOException {
    final int numBuckets = in.readInt();
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
    final String name = readString(in);
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

  private static Map<String, ColumnStore> readOptionalColumnStoreMap(final DataInputStream in) throws IOException {
    if (!in.readBoolean())
      return null;
    final int count = in.readInt();
    final Map<String, ColumnStore> result = new LinkedHashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++)
      result.put(readString(in), readColumnStore(in));
    return result;
  }

  private static Map<String, int[]> readOptionalIntArrayMap(final DataInputStream in) throws IOException {
    if (!in.readBoolean())
      return null;
    final int count = in.readInt();
    final Map<String, int[]> result = new LinkedHashMap<>(Math.max(16, count * 2));
    for (int i = 0; i < count; i++)
      result.put(readString(in), readIntArray(in));
    return result;
  }

  /**
   * Reads the length written by {@link #writeIntArray} followed by one bulk read and one bulk {@link ByteBuffer}
   * conversion, mirroring the write side.
   */
  private static int[] readIntArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    final int[] array = new int[len];
    if (len == 0)
      return array;
    final byte[] bytes = new byte[len * Integer.BYTES];
    in.readFully(bytes);
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asIntBuffer().get(array);
    return array;
  }

  private static long[] readLongArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    final long[] array = new long[len];
    if (len == 0)
      return array;
    final byte[] bytes = new byte[len * Long.BYTES];
    in.readFully(bytes);
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asLongBuffer().get(array);
    return array;
  }

  private static double[] readDoubleArray(final DataInputStream in) throws IOException {
    final int len = in.readInt();
    final double[] array = new double[len];
    if (len == 0)
      return array;
    final byte[] bytes = new byte[len * Double.BYTES];
    in.readFully(bytes);
    ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).asDoubleBuffer().get(array);
    return array;
  }
}
