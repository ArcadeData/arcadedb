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
package com.arcadedb.graph;

import com.arcadedb.database.BaseRecord;
import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.RecordInternal;
import com.arcadedb.exception.DatabaseMetadataException;
import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Directory record of a "super-node" (hot vertex) striped edge list. When a vertex crosses the
 * {@link com.arcadedb.GlobalConfiguration#GRAPH_SUPERNODE_THRESHOLD} its head-chunk pointer is flipped from the
 * plain {@link EdgeSegment} chain to one of these records, which lists the head chunk of every STRIPE the edge
 * list is spread over. Stripes live in different files, so concurrent appends to the same hot vertex take
 * different per-file commit locks and proceed in parallel instead of serialising on one page.
 * <p>
 * The record is organised in GENERATIONS so the stripe count can grow later without ever moving data:
 * generation 0 is always the pre-promotion classic chain (1 stripe), generation 1 holds the stripes created at
 * promotion, and a future growth simply appends a generation. Appends always go to the NEWEST generation;
 * lookups by neighbour visit one stripe per generation. Generations hold disjoint entries, so counts and
 * iterations simply sum/concatenate all chains.
 * <p>
 * Binary layout (plain fixed-width values - the record is small, rarely written, and slots are updated
 * in place):
 * <pre>
 * [type:1][hashVersion:1][generations:1] then per generation: [stripes:int][stripes x (bucketId:int, position:long)]
 * </pre>
 * A slot of {@code (-1, -1)} means the stripe has no chain yet (lazily allocated on its first append).
 * <p>
 * The record is almost immutable: after creation it is rewritten only when a stripe's head chunk fills (the new
 * chunk becomes that stripe's head) or on a stripe's lazy first use - about once per ~1000 appends per stripe -
 * so its page never becomes a hot spot itself.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class StripeDirectory extends BaseRecord implements RecordInternal {
  public static final byte RECORD_TYPE  = 7;
  /**
   * Version of the {@link #stripeOf(RID, int)} placement function used by this record. The hash is part of the
   * on-disk contract (entries are found by re-hashing), so it can never change silently: a new algorithm must
   * use a new version number and keep the old one readable.
   * <p>
   * Every directory built from stored bytes is checked against {@link #isSupportedHashVersion(byte)}
   * ({@link #checkHashVersion()}): a version this release does not know FAILS CLOSED with a
   * {@link DatabaseMetadataException}, the same way an unknown record type does in the {@code RecordFactory}. Reading
   * it with the version-0 hash would look in the wrong stripe and silently answer "not connected" or miss a removal
   * (#8868).
   * <p>
   * Releases older than the one that added this check do NOT validate the byte, and read any version with the
   * version-0 hash. A new placement version is therefore only safe once those releases are out of support; until
   * then a layout that changes placement needs a new record type, which they reject in the {@code RecordFactory}.
   */
  public static final byte HASH_VERSION = 0;

  /**
   * Every placement hash version this release can read (#9266). {@link #HASH_VERSION} is the one it WRITES; a
   * release that introduces a second placement function adds it here and keeps the old one, so directories written
   * before the switch stay readable. Checked by {@link #isSupportedHashVersion(byte)}, never by an equality on
   * {@link #HASH_VERSION}.
   */
  private static final byte[] SUPPORTED_HASH_VERSIONS = { HASH_VERSION };

  private static final int  HEADER_SIZE = 3;                                 // type + hashVersion + generations
  private static final int  SLOT_SIZE   = Binary.INT_SERIALIZED_SIZE + Binary.LONG_SERIALIZED_SIZE;
  private static final long NULL_SLOT   = -1L;

  private int bufferSize;

  /**
   * Loads an existing directory from its persistent buffer (invoked by the {@code RecordFactory}). A null
   * buffer creates a LAZY placeholder (the generic no-content factory path): the content is loaded on first
   * access by {@link #checkForLoading()}.
   */
  public StripeDirectory(final Database database, final RID rid, final Binary buffer) {
    super(database, rid, buffer);
    if (buffer != null) {
      checkHashVersion();
      this.buffer.setAutoResizable(false);
      this.bufferSize = buffer.size();
    } else
      this.bufferSize = 0;
  }

  /**
   * Creates a new directory for the promotion of a vertex: generation 0 = the existing classic chain,
   * generation 1 = {@code stripes} empty slots (lazily allocated on first append).
   */
  public StripeDirectory(final DatabaseInternal database, final RID classicHeadRID, final int stripes) {
    super(database, null, new Binary(HEADER_SIZE + 2 * Binary.INT_SERIALIZED_SIZE + (1 + stripes) * SLOT_SIZE));
    this.buffer.setAutoResizable(false);
    this.bufferSize = buffer.capacity();

    buffer.putByte(0, RECORD_TYPE);
    buffer.putByte(1, HASH_VERSION);
    buffer.putByte(2, (byte) 2); // generations

    // GENERATION 0: THE PRE-PROMOTION CLASSIC CHAIN
    int offset = HEADER_SIZE;
    buffer.putInt(offset, 1);
    offset += Binary.INT_SERIALIZED_SIZE;
    buffer.putInt(offset, classicHeadRID.getBucketId());
    buffer.putLong(offset + Binary.INT_SERIALIZED_SIZE, classicHeadRID.getPosition());
    offset += SLOT_SIZE;

    // GENERATION 1: THE STRIPES, ALL LAZY
    buffer.putInt(offset, stripes);
    offset += Binary.INT_SERIALIZED_SIZE;
    for (int i = 0; i < stripes; i++) {
      buffer.putInt(offset, -1);
      buffer.putLong(offset + Binary.INT_SERIALIZED_SIZE, NULL_SLOT);
      offset += SLOT_SIZE;
    }
  }

  /**
   * Deterministic stripe placement by the NEIGHBOUR vertex RID. FROZEN CONTRACT (see {@link #HASH_VERSION}):
   * entries are located by re-hashing, so this function can never change for version 0. Mixes both RID
   * components with a murmur3-style finaliser so strided/sequential positions do not alias on the modulo.
   */
  public static int stripeOf(final RID neighbour, final int stripes) {
    long h = neighbour.getPosition() * 0x9E3779B97F4A7C15L;
    h ^= (long) neighbour.getBucketId() * 0xC2B2AE3D27D4EB4FL;
    h ^= h >>> 33;
    h *= 0xFF51AFD7ED558CCDL;
    h ^= h >>> 33;
    return (int) ((h & Long.MAX_VALUE) % stripes);
  }

  /**
   * Loads the record content when this instance was created as a lazy placeholder (no-content factory path).
   * SYNCHRONIZED: a lazy placeholder from the generic factory can be shared across reader threads, and the
   * lazy load writes non-volatile fields - the monitor makes the load-then-publish safe. Uncontended cost on
   * the loaded fast path is a thin lock, negligible next to the page access that follows.
   */
  private synchronized void checkForLoading() {
    if (buffer == null)
      reload();
  }

  /**
   * Re-reads the content from its bucket and validates it ({@link #checkHashVersion()}). Every lazy load funnels
   * through here - {@link #checkForLoading()} and {@code BaseRecord.size()} alike - so no caller can leave unvalidated
   * content behind for the next accessor to read with the wrong placement hash. A rejected content is DROPPED before
   * the exception is rethrown: a second access must fail again, not find a loaded buffer.
   */
  @Override
  public synchronized void reload() {
    super.reload();
    if (buffer != null) {
      try {
        checkHashVersion();
      } catch (final DatabaseMetadataException e) {
        buffer = null;
        throw e;
      }
      buffer.setAutoResizable(false);
      bufferSize = buffer.size();
    }
  }

  /**
   * Rejects a directory this release cannot read: a placement hash version it does not support, a header shorter
   * than {@link #HEADER_SIZE}, or a body too short for the generations and stripes its header declares. Absolute
   * reads only: does not move the buffer position the record factory left behind.
   */
  private void checkHashVersion() {
    final String problem = describeUnreadableContent(buffer);
    if (problem != null)
      throw new DatabaseMetadataException("Stripe directory " + rid + " " + problem);
  }

  /** True when this release implements the placement function of {@code version} (#9266). */
  public static boolean isSupportedHashVersion(final byte version) {
    for (final byte supported : SUPPORTED_HASH_VERSIONS)
      if (supported == version)
        return true;
    return false;
  }

  /**
   * Explains why the stored content of a stripe directory cannot be read by this release, or returns {@code null}
   * when it can. Shared by the loading paths, which turn the answer into a {@link DatabaseMetadataException}, and by
   * {@code CHECK DATABASE}, which reports it as a per-record finding instead of failing (#9266).
   * <p>
   * The layout is only verified for a supported version: an unknown version may lay its body out differently, so
   * its slots are never decoded.
   *
   * @param content the record content, starting with the record type byte, sized to the record
   */
  public static String describeUnreadableContent(final Binary content) {
    final int size = content.size();
    if (size < HEADER_SIZE)
      return "is corrupt: " + size + " bytes, shorter than its " + HEADER_SIZE + "-byte header";

    final byte version = content.getByte(1);
    if (!isSupportedHashVersion(version))
      return "uses placement hash version " + version + ", but this release only supports " + supportedHashVersionsAsString()
          + ". Either the database was written by a newer ArcadeDB release (open it with that release or a later one) or the "
          + "record is corrupt";

    final int generations = content.getByte(2);
    if (generations < 1)
      return "is corrupt: its header declares " + generations + " generations";

    int offset = HEADER_SIZE;
    for (int g = 0; g < generations; g++) {
      if (offset + Binary.INT_SERIALIZED_SIZE > size)
        return "is truncated: " + size + " bytes, generation " + g + " of " + generations + " starts past the end";
      final int stripes = content.getInt(offset);
      if (stripes < 1)
        return "is corrupt: generation " + g + " declares " + stripes + " stripes";
      offset += Binary.INT_SERIALIZED_SIZE;
      if ((long) offset + (long) stripes * SLOT_SIZE > size)
        return "is truncated: " + size + " bytes, generation " + g + " declares " + stripes + " stripes that do not fit";
      offset += stripes * SLOT_SIZE;
    }
    return null;
  }

  private static String supportedHashVersionsAsString() {
    if (SUPPORTED_HASH_VERSIONS.length == 1)
      return "version " + SUPPORTED_HASH_VERSIONS[0];
    final StringBuilder versions = new StringBuilder("versions ");
    for (int i = 0; i < SUPPORTED_HASH_VERSIONS.length; i++) {
      if (i > 0)
        versions.append(", ");
      versions.append(SUPPORTED_HASH_VERSIONS[i]);
    }
    return versions.toString();
  }

  /** Bounded by the single-byte header field: at most 127 generations (a growth event is rare - promotion adds
   * one, a future re-stripe would add one - so the bound is practically unreachable; a generation-append must
   * still guard it). */
  public int getGenerationCount() {
    checkForLoading();
    return buffer.getByte(2);
  }

  public int getNewestGeneration() {
    return getGenerationCount() - 1;
  }

  public int getStripes(final int generation) {
    checkForLoading();
    return buffer.getInt(generationOffset(generation));
  }

  /** Returns the head chunk RID of the given stripe, or {@code null} if the stripe has no chain yet. */
  public RID getHead(final int generation, final int slot) {
    checkForLoading();
    final int offset = slotOffset(generation, slot);
    final long position = buffer.getLong(offset + Binary.INT_SERIALIZED_SIZE);
    if (position == NULL_SLOT)
      return null;
    return new RID(buffer.getInt(offset), position);
  }

  /** Updates a stripe's head chunk in place (fixed-width slot: the record size never changes). */
  public void setHead(final int generation, final int slot, final RID head) {
    checkForLoading();
    final int offset = slotOffset(generation, slot);
    buffer.putInt(offset, head.getBucketId());
    buffer.putLong(offset + Binary.INT_SERIALIZED_SIZE, head.getPosition());
  }

  /** Total number of chains (non-null heads) across all generations. */
  public int getChainCount() {
    int total = 0;
    for (int g = 0; g < getGenerationCount(); g++)
      for (int s = 0; s < getStripes(g); s++)
        if (getHead(g, s) != null)
          total++;
    return total;
  }

  private int generationOffset(final int generation) {
    int offset = HEADER_SIZE;
    for (int g = 0; g < generation; g++)
      offset += Binary.INT_SERIALIZED_SIZE + buffer.getInt(offset) * SLOT_SIZE;
    return offset;
  }

  private int slotOffset(final int generation, final int slot) {
    return generationOffset(generation) + Binary.INT_SERIALIZED_SIZE + slot * SLOT_SIZE;
  }

  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  /** NOTE: mutates the buffer's position/limit (idempotent, but not re-entrant-safe across threads - directory
   * instances are never shared across threads on the write path). */
  public Binary getContent() {
    checkForLoading();
    buffer.position(bufferSize);
    buffer.flip();
    return buffer;
  }

  @Override
  public void setIdentity(final RID rid) {
    this.rid = rid;
  }

  @Override
  public void unsetDirty() {
    // IGNORE THIS FLAG
  }

  @Override
  public JSONObject toJSON(final boolean includeMetadata) {
    final JSONObject json = new JSONObject();
    final JSONArray generations = new JSONArray();
    for (int g = 0; g < getGenerationCount(); g++) {
      final JSONArray slots = new JSONArray();
      for (int s = 0; s < getStripes(g); s++) {
        final RID head = getHead(g, s);
        slots.put(head != null ? head.toString() : null);
      }
      generations.put(slots);
    }
    json.put("stripes", generations);
    if (includeMetadata) {
      json.put("@cat", "sd");
      if (rid != null)
        json.put("@rid", rid.toString());
    }
    return json;
  }
}
