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
package com.arcadedb.index.sparsevector;

import com.arcadedb.database.RID;
import com.arcadedb.index.sparsevector.SegmentFormat.RidCompression;
import com.arcadedb.index.sparsevector.SegmentFormat.WeightQuantization;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Random-access reader for {@code .sparseseg} sealed segment files.
 * <p>
 * The reader opens the file, validates the header CRC, parses the manifest and dim_index, and
 * loads each accessed dim's metadata on demand. Postings are decoded block by block via
 * {@link SegmentDimCursor}. Block headers are reachable independently of payload decompression to
 * support the BlockMax-WAND skip primitive.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public final class SparseSegmentReader implements AutoCloseable {

  private final Path              file;
  private final FileChannel       channel;
  private final SegmentParameters params;
  private final long              manifestOffset;
  private final long              totalPostings;
  private final int               totalDims;
  private final long              segmentId;
  private final long[]            parentSegments;
  private final long              tombstoneFloorSegment;
  private final long              dimIndexOffset;
  private final Map<Integer, Long> dimHeaderOffsets;
  // Cached per-dim metadata. Keyed by dim_id. Loaded lazily, never evicted.
  private final Map<Integer, DimMetadata> dimCache = new HashMap<>();

  private boolean closed;

  public SparseSegmentReader(final Path file) throws IOException {
    this.file = file;
    this.channel = FileChannel.open(file, StandardOpenOption.READ);

    // ---- File header ----
    final ByteBuffer header = ByteBuffer.allocate(SegmentFormat.HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    final int read = channel.read(header, 0L);
    if (read != SegmentFormat.HEADER_SIZE)
      throw new IOException("Truncated segment header in " + file + ": got " + read + " bytes");
    header.flip();

    final long magic = header.getLong(SegmentFormat.HEADER_OFFSET_MAGIC);
    if (magic != SegmentFormat.MAGIC)
      throw new IOException("Bad magic in " + file + ": expected ASPV0001, got " + Long.toHexString(magic));

    final int formatVersion = header.getInt(SegmentFormat.HEADER_OFFSET_FORMAT_VERSION);
    if (formatVersion != SegmentFormat.FORMAT_VERSION)
      throw new IOException("Unsupported format version in " + file + ": " + formatVersion);

    final int storedCrc = header.getInt(SegmentFormat.HEADER_OFFSET_CRC32);
    final CRC32 crc = new CRC32();
    crc.update(header.array(), 0, SegmentFormat.HEADER_OFFSET_CRC32);
    if ((int) crc.getValue() != storedCrc)
      throw new IOException("Header CRC mismatch in " + file + " (file truncated or corrupt)");

    final int pageSize = header.getInt(SegmentFormat.HEADER_OFFSET_PAGE_SIZE);
    final int blockSize = header.getInt(SegmentFormat.HEADER_OFFSET_BLOCK_SIZE);
    final int skipStride = header.getInt(SegmentFormat.HEADER_OFFSET_SKIP_STRIDE);
    final WeightQuantization wq = WeightQuantization.fromCode(header.get(SegmentFormat.HEADER_OFFSET_WEIGHT_QUANTIZATION));
    final RidCompression rc = RidCompression.fromCode(header.get(SegmentFormat.HEADER_OFFSET_RID_COMPRESSION));

    this.params = SegmentParameters.builder()
        .pageSize(pageSize)
        .blockSize(blockSize)
        .skipStride(skipStride)
        .weightQuantization(wq)
        .ridCompression(rc)
        .build();
    this.manifestOffset = header.getLong(SegmentFormat.HEADER_OFFSET_MANIFEST_OFFSET);
    this.totalPostings = header.getLong(SegmentFormat.HEADER_OFFSET_TOTAL_POSTINGS);
    this.totalDims = header.getInt(SegmentFormat.HEADER_OFFSET_TOTAL_DIMS);

    // ---- Manifest ----
    final ByteBuffer manifestPrefix = ByteBuffer.allocate(8 + 8 + 4).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(manifestPrefix, manifestOffset) != manifestPrefix.capacity())
      throw new IOException("Truncated manifest in " + file);
    manifestPrefix.flip();
    this.dimIndexOffset = manifestPrefix.getLong();
    this.segmentId = manifestPrefix.getLong();
    final int parentCount = manifestPrefix.getInt();

    if (parentCount < 0 || parentCount > 1_000_000)
      throw new IOException("Implausible parent_segments count in manifest of " + file + ": " + parentCount);
    final ByteBuffer manifestRest = ByteBuffer.allocate(parentCount * 8 + 8 + 8 + 4).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(manifestRest, manifestOffset + 8 + 8 + 4) != manifestRest.capacity())
      throw new IOException("Truncated manifest body in " + file);
    manifestRest.flip();
    this.parentSegments = new long[parentCount];
    for (int i = 0; i < parentCount; i++)
      this.parentSegments[i] = manifestRest.getLong();
    this.tombstoneFloorSegment = manifestRest.getLong();
    manifestRest.getLong(); // reserved
    final int storedManifestCrc = manifestRest.getInt();

    // Validate manifest CRC over the entire manifest record (prefix + rest minus the trailing 4-byte CRC).
    final ByteBuffer crcView = ByteBuffer.allocate(manifestPrefix.capacity() + manifestRest.capacity() - 4).order(ByteOrder.LITTLE_ENDIAN);
    crcView.put(manifestPrefix.array(), 0, manifestPrefix.capacity());
    crcView.put(manifestRest.array(), 0, manifestRest.capacity() - 4);
    final CRC32 manifestCrc = new CRC32();
    manifestCrc.update(crcView.array(), 0, crcView.capacity());
    if ((int) manifestCrc.getValue() != storedManifestCrc)
      throw new IOException("Manifest CRC mismatch in " + file);

    // ---- Dim index ----
    final ByteBuffer dimCount = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(dimCount, dimIndexOffset) != 4)
      throw new IOException("Truncated dim_index header in " + file);
    dimCount.flip();
    final int n = dimCount.getInt();
    if (n != totalDims)
      throw new IOException("Inconsistent dim count in " + file + ": header says " + totalDims + ", dim_index says " + n);

    final ByteBuffer entries = ByteBuffer.allocate(n * (4 + 8)).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(entries, dimIndexOffset + 4) != entries.capacity())
      throw new IOException("Truncated dim_index entries in " + file);
    entries.flip();
    this.dimHeaderOffsets = new HashMap<>(Math.max(8, n * 2));
    int prevDim = Integer.MIN_VALUE;
    for (int i = 0; i < n; i++) {
      final int dimId = entries.getInt();
      final long offset = entries.getLong();
      if (dimId <= prevDim)
        throw new IOException("dim_index not sorted in " + file + ": " + dimId + " after " + prevDim);
      prevDim = dimId;
      this.dimHeaderOffsets.put(dimId, offset);
    }
  }

  public Path file() {
    return file;
  }

  public SegmentParameters parameters() {
    return params;
  }

  public long totalPostings() {
    return totalPostings;
  }

  public int totalDims() {
    return totalDims;
  }

  public long segmentId() {
    return segmentId;
  }

  public long[] parentSegments() {
    return parentSegments.clone();
  }

  public long tombstoneFloorSegment() {
    return tombstoneFloorSegment;
  }

  public boolean hasDim(final int dimId) {
    return dimHeaderOffsets.containsKey(dimId);
  }

  /** Returns dim_ids present in this segment, sorted ascending. */
  public int[] dims() {
    final int[] out = new int[dimHeaderOffsets.size()];
    int i = 0;
    for (final int d : dimHeaderOffsets.keySet())
      out[i++] = d;
    java.util.Arrays.sort(out);
    return out;
  }

  /** Returns the metadata for {@code dimId}, loading from disk if needed. {@code null} if absent. */
  public DimMetadata dimMetadata(final int dimId) throws IOException {
    DimMetadata m = dimCache.get(dimId);
    if (m != null)
      return m;
    final Long offset = dimHeaderOffsets.get(dimId);
    if (offset == null)
      return null;
    m = loadDimMetadata(dimId, offset);
    dimCache.put(dimId, m);
    return m;
  }

  /** Opens a cursor that walks the postings of {@code dimId} in ascending RID order. {@code null} if dim absent. */
  public SegmentDimCursor openCursor(final int dimId) throws IOException {
    final DimMetadata md = dimMetadata(dimId);
    if (md == null)
      return null;
    return new SegmentDimCursor(this, md);
  }

  @Override
  public void close() throws IOException {
    if (closed)
      return;
    closed = true;
    channel.close();
  }

  // ---------- internals ----------

  FileChannel channel() {
    return channel;
  }

  private DimMetadata loadDimMetadata(final int dimId, final long dimHeaderOffset) throws IOException {
    // Read the fixed-size dim_header prefix first to learn block_count.
    final ByteBuffer hdr = ByteBuffer.allocate(4 + 4 + 4 + 4 + 4).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(hdr, dimHeaderOffset) != hdr.capacity())
      throw new IOException("Truncated dim_header for dim " + dimId);
    hdr.flip();
    final int storedDimId = hdr.getInt();
    if (storedDimId != dimId)
      throw new IOException("dim_header dim_id mismatch: expected " + dimId + " got " + storedDimId);
    final int blockCount = hdr.getInt();
    final int postingCount = hdr.getInt();
    final int df = hdr.getInt();
    final float globalMaxWeight = hdr.getFloat();

    if (blockCount <= 0 || blockCount > 1_000_000_000)
      throw new IOException("Implausible block_count for dim " + dimId + ": " + blockCount);

    // Read block_offsets + skip_list together.
    final int skipEntries = (blockCount + params.skipStride() - 1) / params.skipStride();
    final int trailerSize = blockCount * 8 + skipEntries * SegmentFormat.SKIP_ENTRY_SIZE;
    final ByteBuffer trailer = ByteBuffer.allocate(trailerSize).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(trailer, dimHeaderOffset + hdr.capacity()) != trailerSize)
      throw new IOException("Truncated dim trailer for dim " + dimId);
    trailer.flip();

    final long[] blockOffsets = new long[blockCount];
    for (int b = 0; b < blockCount; b++)
      blockOffsets[b] = trailer.getLong();

    final SkipEntry[] skipList = new SkipEntry[skipEntries];
    for (int s = 0; s < skipEntries; s++) {
      final int bucketId = trailer.getInt();
      final long pos = trailer.getLong();
      final float maxToEnd = trailer.getFloat();
      final int blockIndex = trailer.getInt();
      skipList[s] = new SkipEntry(new RID(bucketId, pos), maxToEnd, blockIndex);
    }

    // Eagerly load block headers (small) so cursors can evaluate block-skip without extra I/O round-trips.
    final BlockHeader[] blockHeaders = new BlockHeader[blockCount];
    for (int b = 0; b < blockCount; b++)
      blockHeaders[b] = readBlockHeader(blockOffsets[b]);

    return new DimMetadata(dimId, postingCount, df, globalMaxWeight, blockOffsets, blockHeaders, skipList);
  }

  BlockHeader readBlockHeader(final long offset) throws IOException {
    final ByteBuffer buf = ByteBuffer.allocate(SegmentFormat.BLOCK_HEADER_SIZE).order(ByteOrder.LITTLE_ENDIAN);
    if (channel.read(buf, offset) != buf.capacity())
      throw new IOException("Truncated block header at offset " + offset);
    buf.flip();
    final int firstBucket = buf.getInt();
    final long firstPos = buf.getLong();
    final int lastBucket = buf.getInt();
    final long lastPos = buf.getLong();
    final int postingCount = buf.getShort() & 0xFFFF;
    final float maxWeight = buf.getFloat();
    final float weightMin = buf.getFloat();
    final float weightMax = buf.getFloat();
    final boolean hasTombstones = buf.get() != 0;
    buf.get(); // reserved
    return new BlockHeader(new RID(firstBucket, firstPos), new RID(lastBucket, lastPos), postingCount, maxWeight, weightMin,
        weightMax, hasTombstones);
  }

  /** Reads the compressed RID + weight payload that immediately follows the block header at {@code blockOffset}. */
  ByteBuffer readBlockPayload(final long blockOffset, final int payloadEstimate) throws IOException {
    // The payload starts immediately after the block header. Its length is bounded by:
    //   (blockSize - 1) * 2 * MAX_VARLONG_BYTES (RID deltas, varint-zigzag) + blockSize * sizeof(weight).
    // We read the worst-case slice; trailing bytes are unused (fine for ByteBuffer scanning).
    final ByteBuffer buf = ByteBuffer.allocate(payloadEstimate).order(ByteOrder.LITTLE_ENDIAN);
    final int read = channel.read(buf, blockOffset + SegmentFormat.BLOCK_HEADER_SIZE);
    if (read < 0)
      throw new IOException("Read past EOF at block offset " + blockOffset);
    buf.flip();
    return buf;
  }

  int maxBlockPayloadSize() {
    final int weightBytes = switch (params.weightQuantization()) {
      case INT8 -> params.blockSize();
      case FP16 -> params.blockSize() * 2;
      case FP32 -> params.blockSize() * 4;
    };
    return (params.blockSize() - 1) * 2 * VarInt.MAX_VARLONG_BYTES + weightBytes + 64;
  }
}
