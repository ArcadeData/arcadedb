/*
 * Copyright 2021-present Arcade Data Ltd (info@arcadedata.com)
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
package com.arcadedb.server.ha.raft;

import com.arcadedb.compression.CompressionFactory;
import com.arcadedb.network.binary.ReplicatedEntryTooLargeException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * Codec for encoding and decoding Raft log entries. Converts WAL transaction data
 * and schema commands into Ratis ByteString representations and back.
 */
public final class RaftLogEntryCodec {

  /**
   * Ceiling a PRODUCER applies to the uncompressed payload it packs into one replicated entry (64 MB).
   * <p>
   * Issue #5933: the submit-time gate in {@code RaftGroupCommitter.submitAndWait} measures the ENCODED entry,
   * whose WAL this codec has already LZ4-compressed, while the applier has to materialize that WAL again in
   * full. A well-compressing bulk transaction - repetitive, text/JSON-heavy migration data - therefore used to
   * pass the submit gate on its compressed size, reach a Raft quorum, commit at a fixed log index, and only
   * then fail the decode-time bound on every node that tried to apply it. A committed entry cannot be taken
   * back, so every node halted on it and replayed into the same failure on every restart: a permanent,
   * cluster-wide crash loop from a single oversized write.
   * <p>
   * The gates now agree by construction: nothing is encoded that could not be decoded. This value has to stay
   * at or above {@code GlobalConfiguration.maxReplicatedRaftEntrySize}'s default, or an INCOMPRESSIBLE
   * transaction that replicates fine today (raw size ~= compressed size) would start being rejected here.
   */
  static final int MAX_ENTRY_BYTES = 64 * 1024 * 1024;

  /**
   * Ceiling a CONSUMER accepts when decoding (512 MB), deliberately well above {@link #MAX_ENTRY_BYTES}.
   * <p>
   * Being liberal in what is accepted is what lets a cluster ALREADY holding an oversized committed entry -
   * written by a version that had no producer-side bound - apply it and come back up, instead of crash-looping
   * on it forever with no operator-side recovery. It is also why the two ceilings must never be equal: a
   * decoder no more permissive than the encoder leaves no margin for entries produced by another version.
   * <p>
   * This is a wire-format constant and NOT configurable on purpose. A node whose ceiling differed from its
   * peers' would apply entries they reject, which is exactly the divergence the state machine halts to
   * prevent. {@code RaftPropertiesBuilder} refuses at startup any configured entry cap above it.
   */
  static final int MAX_DECODED_ENTRY_BYTES = 512 * 1024 * 1024;

  /**
   * Upper bound on how far the LZ4 block format can expand its input, used to reject an absurd uncompressed
   * length BEFORE the array it asks for is allocated. A block sequence spends at least a token byte plus a
   * two-byte offset per match, and every additional length-extension byte adds at most 255 output bytes, so
   * the ratio converges to - and never reaches - 255:1, so a well-formed block can never trip this bound.
   * Real WAL pages sit in the low single digits, so in practice it only ever fires on a corrupt or hostile
   * length field.
   * <p>
   * It is tied to the block format of {@code CompressionFactory.getDefault()}, which every section here is
   * compressed with: swapping that for an algorithm with a higher maximum ratio means revisiting this value
   * FIRST, or entries a peer legitimately produced would be refused - and refusing a committed entry is the
   * cluster-wide crash loop #5933 is about.
   */
  private static final int MAX_LZ4_EXPANSION_RATIO = 255;

  /** Maximum allowed element count for collections during decoding. */
  static final int MAX_COLLECTION_SIZE = 1_000_000;

  /**
   * Frame marker introducing a trailing EXTENSION SECTION: the forward-compatibility mechanism every entry type
   * gets from this release on (issue #7138).
   * <p>
   * There is no version field on a Raft log entry - the type byte is the whole envelope - and forward
   * compatibility used to be per-type ad hoc. {@code SCHEMA_ENTRY} carries self-describing optional sections and
   * tolerates them; for every other type an unknown trailing field was FATAL, and fatal in the worst possible
   * arm: the throw happens in {@code RaftLogEntryCodec.decode} outside {@code applyWithRetry}, so it is not a
   * {@code NeedRetryException}, not a {@code ReplicationException} and not quarantinable - it reached
   * {@code catch (Throwable)} and halted the node, with the applied index deliberately not advanced, so the halt
   * repeated on every restart. A newer leader adding one field to, say, {@code DROP_DATABASE_ENTRY} would
   * therefore have permanently halted every not-yet-upgraded peer: on a 3-node rolling upgrade, two nodes down.
   * <p>
   * A version or length prefix on the envelope cannot fix this, because no already-deployed decoder knows to
   * look for one - the only change that helps is one existing decoders can act on, which means tolerating what
   * comes AFTER what they understand. Tolerating anything trailing (what {@code SCHEMA_ENTRY} does) would do it,
   * at the cost of the corruption signal this check exists for. Framing keeps both: a future field appended as
   * {@code [magic][length][payload]} is recognisably a deliberate extension and is skipped, while a truncated or
   * corrupt entry does not carry the magic and is still refused.
   * <p>
   * <b>Contract for whoever adds the next field:</b> append it with {@link #writeExtensionSection} - never as
   * bare trailing bytes - and make its absence mean "the peer that wrote this predates the field". Peers running
   * a version older than this one still halt on it, so a field may only be EMITTED once every peer in the
   * supported upgrade range tolerates extensions; from that release on, adding a field is safe by construction.
   * The value is the ASCII {@code "ADBX"}, chosen so it is recognisable in a hex dump of a log segment.
   * <p>
   * <b>{@code SCHEMA_ENTRY} is excluded, and not merely exempt.</b> Its optional sections are UNFRAMED and
   * positional - a decoder that has consumed the file maps reads whatever comes next as the #4382 WAL section's
   * count - so a frame appended to one is not skipped, it is read as that count and rejected as a corrupt
   * entry. Extending {@code SCHEMA_ENTRY} means adding another section to its own mechanism, in
   * {@code decodeSchemaEntry}, exactly as #5443 and #4416 did.
   */
  static final int EXTENSION_MAGIC = 0x41444258;

  /** Bytes one extension frame spends on its header ({@link #EXTENSION_MAGIC} + length). */
  private static final int EXTENSION_HEADER_BYTES = 8;

  /**
   * Appends one self-describing extension section to an entry being encoded. Call this AFTER the type's own
   * fields have been written, so a decoder that predates the section stops cleanly at the end of what it knows.
   * Sections may be repeated; a decoder skips every one it does not recognise.
   * <p>
   * Not for {@code SCHEMA_ENTRY}, whose own unframed optional sections would swallow the frame header - see
   * {@link #EXTENSION_MAGIC}.
   *
   * @param payload the section's bytes, whose meaning is entirely up to the field being added (typically its own
   *                small self-describing sub-format, so that a future reader can tell its sections apart)
   */
  public static void writeExtensionSection(final DataOutputStream dos, final byte[] payload) throws IOException {
    dos.writeInt(EXTENSION_MAGIC);
    dos.writeInt(payload.length);
    dos.write(payload);
  }

  /**
   * {@link #writeExtensionSection} for an entry that has already been encoded, returning a new {@link ByteString}.
   * Copies the entry, so an encoder writing its own fields should prefer the streaming form; this exists for
   * callers holding a finished entry (and for the tests that prove old decoders tolerate a new field).
   */
  public static ByteString appendExtensionSection(final ByteString entry, final byte[] payload) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream(entry.size() + EXTENSION_HEADER_BYTES + payload.length);
      final DataOutputStream dos = new DataOutputStream(baos);
      entry.writeTo(dos);
      writeExtensionSection(dos, payload);
      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to append an extension section to a Raft log entry", e);
    }
  }

  private RaftLogEntryCodec() {
    // utility class
  }

  private static void checkByteLength(final int length, final String context) {
    if (length < 0 || length > MAX_DECODED_ENTRY_BYTES)
      throw new IllegalStateException(
          "Invalid byte length " + length + " in " + context + " (max " + MAX_DECODED_ENTRY_BYTES + ")");
  }

  /**
   * Validates a length field for a section read STRAIGHT off the stream, so the array it asks for is never
   * allocated before the entry is known to actually carry that many bytes.
   * <p>
   * {@code remaining} is the exact number of bytes left in the entry: the stream is backed by a
   * {@link ByteString}, whose {@code available()} is exact and is already load-bearing in {@link #decode}
   * (the trailing-byte corruption check and the optional-section probes both depend on it). That makes it a
   * far tighter bound than any constant - a corrupt or forged length is refused here rather than turning into
   * a several-hundred-MB allocation that {@code readFully} would then fail on anyway. The absolute ceiling
   * stays as a backstop.
   * <p>
   * This bound does NOT apply to the length of a section that is decompressed, which is legitimately many
   * times the bytes present; {@link #checkDecompressedLength} covers that one.
   */
  private static void checkByteLength(final int length, final int remaining, final String context) {
    checkByteLength(length, context);
    if (length > remaining)
      throw new IllegalStateException("Invalid byte length " + length + " in " + context + ": only " + remaining
          + " bytes remain in the entry (truncated or corrupted replication payload)");
  }

  /**
   * Validates the uncompressed length a compressed section claims, before {@code decompress} allocates it.
   * Beyond the absolute ceiling the claim is also bounded by what the compressed bytes present in the entry
   * could possibly expand to, so raising {@link #MAX_DECODED_ENTRY_BYTES} cannot turn the decoder into an
   * allocation amplifier for a corrupt length field.
   */
  private static void checkDecompressedLength(final int uncompressedLength, final int compressedLength,
      final String context) {
    checkByteLength(uncompressedLength, context);
    if ((long) uncompressedLength > (long) compressedLength * MAX_LZ4_EXPANSION_RATIO)
      throw new IllegalStateException("Invalid byte length " + uncompressedLength + " in " + context
          + ": above the " + MAX_LZ4_EXPANSION_RATIO + ":1 maximum expansion ratio of the " + compressedLength
          + " compressed bytes carried by the entry");
  }

  /**
   * Producer-side bound, applied to every section this codec COMPRESSES (issue #5933). The submit-time gate
   * only ever sees the compressed result, so this is the one place a payload that no node could decompress can
   * still be stopped before it reaches the Raft log. The failure is a {@link ReplicatedEntryTooLargeException}
   * - a {@code TransactionException} and NOT a {@code NeedRetryException} - because retrying resubmits the
   * identical, equally undeliverable payload.
   */
  private static void checkProducedPayloadLength(final int length, final String databaseName, final String context) {
    checkProducedPayloadLength(length, databaseName, context,
        "Reduce the batch size - fewer rows per GraphBatch / SQL transaction, or smaller records.");
  }

  /**
   * @param remediation what the operator can actually DO about it, because that differs by section and a wrong
   *                    answer here sends them at the wrong knob. A transaction's WAL shrinks by batching less; a
   *                    TimeSeries sealed store does not shrink by batching anything at all (issue #4416) - it
   *                    shrinks by lowering the per-entry sealed cap so the store is sliced more finely, or by
   *                    shortening the type's retention.
   */
  private static void checkProducedPayloadLength(final int length, final String databaseName, final String context,
      final String remediation) {
    if (length > MAX_ENTRY_BYTES)
      throw new ReplicatedEntryTooLargeException(String.format(
          """
          %s for database '%s' is %d bytes uncompressed, above the %d bytes of uncompressed payload a single \
          replicated Raft entry may carry. Compression is not what bounds it: the entry is LZ4-compressed for \
          the wire but every node has to materialize it in full to apply it, so a well-compressing bulk \
          transaction can slip under arcadedb.ha.appendBufferSize and still be too large to apply. %s""",
          context, databaseName, length, MAX_ENTRY_BYTES, remediation));
  }

  /** What an operator does about a TimeSeries sealed payload that no single entry can carry (issue #4416). */
  private static final String SEALED_REMEDIATION =
      "Lower arcadedb.ha.tsMaxSealedInlineSize so the store is sliced more finely, or shorten the retention on "
          + "this TimeSeries type so it holds fewer sealed blocks.";

  /**
   * Rejects a sealed slice whose offsets and lengths cannot describe a real file (issue #4416), HERE rather than
   * where the bytes are written.
   * <p>
   * The applier turns "this sequence does not line up" into a targeted snapshot resync and never into a crash,
   * and a negative offset would have escaped that: it reaches {@code RandomAccessFile.seek} and comes back as a
   * raw {@code IOException} the apply path can only report as an unexpected error. Decoding is where every other
   * malformed-payload check in this class already lives, and it is the one place that sees the field before
   * anything acts on it. The last slice must END exactly at the declared file length - every producer satisfies
   * that by construction, so a payload that does not is not a payload this codec wrote.
   */
  private static void checkSealedSliceGeometry(final String fileName, final long fileLength, final long offset,
      final int sliceLength, final boolean last) {
    final boolean valid = fileLength >= 0 && offset >= 0 && offset + (long) sliceLength <= fileLength
        && (!last || offset + (long) sliceLength == fileLength);
    if (!valid)
      throw new IllegalStateException(
          "Invalid SCHEMA_ENTRY sealed slice for '" + fileName + "': offset " + offset + " + " + sliceLength
              + " bytes does not fit a " + fileLength + "-byte file" + (last ? " it claims to complete" : "")
              + " (corrupted replication payload)");
  }

  private static void checkCollectionSize(final int size, final String context) {
    if (size < 0 || size > MAX_COLLECTION_SIZE)
      throw new IllegalStateException(
          "Invalid collection size " + size + " in " + context + " (max " + MAX_COLLECTION_SIZE + ")");
  }

  public record DecodedEntry(
      RaftLogEntryType type,
      String databaseName,
      byte[] walData,
      Map<Integer, Integer> bucketRecordDelta,
      String schemaJson,
      Map<Integer, String> filesToAdd,
      Map<Integer, String> filesToRemove,
      List<byte[]> walEntries,
      List<Map<Integer, Integer>> bucketDeltas,
      String usersJson,
      boolean forceSnapshot,
      // BOOTSTRAP_FINGERPRINT_ENTRY fields (issue #4147). Hex-encoded SHA-256 of the bootstrap
      // source's database files and the corresponding lastTxId. Null/-1 for non-bootstrap entries.
      String bootstrapFingerprint,
      long bootstrapLastTxId,
      // TimeSeries sealed-store blobs embedded in a SCHEMA_ENTRY (issue #4382). Empty for all other
      // entry types and for SCHEMA_ENTRYs produced by nodes that predate this section.
      List<TsSealedBlob> sealedFileBlobs,
      /**
       * True on every chunk of a SCHEMA_ENTRY that was split across several Raft entries except the last
       * (issue #5443). Such a chunk only DELIVERS pages - the change is published by the final chunk - so
       * the applier must not reload the schema for it. This is carried explicitly rather than inferred:
       * a chunk that creates files but carries no schema JSON is indistinguishable from a legitimate
       * standalone DDL that adds files without changing the schema version, and skipping the reload for
       * THAT would leave the new files unregistered.
       */
      boolean moreChunksFollow,
      /**
       * Slices of a TimeSeries sealed store too large to ride one Raft entry (issue #4416). Empty on every entry
       * whose sealed stores fit inline - those arrive as {@code sealedFileBlobs} - and on entries produced by a
       * node that predates this section.
       */
      List<TsSealedChunk> sealedFileChunks
  ) {
  }

  /**
   * A TimeSeries sealed-store file shipped to followers as part of a compaction/maintenance
   * SCHEMA_ENTRY. The whole file is carried (the smallest safe unit: sealed blocks use cumulative
   * offsets and a rewritten header, so partial appends are unsafe across nodes whose pre-image may
   * differ). The bytes are already decompressed and CRC-verified by the decoder.
   *
   * @param typeName   the TimeSeries type owning the shard
   * @param shardIndex the shard index whose sealed store changed
   * @param fileName   the sealed-store file name relative to the database directory
   * @param bytes      the full sealed-store file content
   */
  public record TsSealedBlob(String typeName, int shardIndex, String fileName, byte[] bytes) {
  }

  /**
   * One slice of a TimeSeries sealed-store file that does not fit a single Raft entry (issue #4416).
   * <p>
   * WHY SLICES AND NOT A SMALLER BLOB. A {@link TsSealedBlob} is the whole file, which made
   * {@code arcadedb.ha.tsMaxSealedInlineSize} a ceiling on the sealed STORE and not merely on one entry: a shard
   * whose store crossed it stopped sealing for good, because the file it would have to ship only ever grows. A
   * sliced store is delivered by a sequence of entries that Raft applies in order on every node, each within the
   * transport limit, so the ceiling becomes the sequence's length rather than one entry's size.
   * <p>
   * The sequence follows the same ordered-prefix contract as {@code RaftTransactionBroker.splitSchemaEntry}: every
   * slice but the last is a delivery-only entry that the follower STAGES on disk and nothing more, and the final
   * slice rides the publishing entry, so the install still happens in the same entry as the mutable-bucket clear
   * WAL and a query can never observe "cleared mutable + stale sealed". A leader that dies mid-sequence leaves a
   * partial staging file, which the next sequence's {@code offset == 0} slice truncates.
   * <p>
   * {@code fileLength} and {@code fileCrc} describe the WHOLE reassembled file and are carried on every slice, so
   * a follower validates what it assembled against the leader's own image before installing it - a per-slice CRC
   * alone would only prove each piece survived the wire.
   *
   * @param typeName   the TimeSeries type owning the shard
   * @param shardIndex the shard index whose sealed store changed
   * @param fileName   the sealed-store file name relative to the database directory, for diagnostics only: the
   *                   follower derives the path it writes from its OWN schema, never from this
   * @param fileLength the length of the whole reassembled file
   * @param fileCrc    CRC32 of the whole reassembled file
   * @param offset     where this slice starts in the reassembled file
   * @param bytes      this slice's bytes, already decompressed and CRC-verified by the decoder
   * @param last       true on the slice that completes the file, i.e. the one the follower installs on
   */
  public record TsSealedChunk(String typeName, int shardIndex, String fileName, long fileLength, long fileCrc,
                              long offset, byte[] bytes, boolean last) {
  }

  /**
   * Encodes a transaction entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF), walData length (int), walData bytes,
   * bucketDelta count (int), followed by pairs of bucketId (int) and delta (int).
   */
  public static ByteString encodeTxEntry(final String databaseName, final byte[] walData,
      final Map<Integer, Integer> bucketRecordDelta) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.TX_ENTRY.getId());
      dos.writeUTF(databaseName);

      checkProducedPayloadLength(walData.length, databaseName, "Transaction WAL");

      final byte[] compressed = CompressionFactory.getDefault().compress(walData);
      dos.writeInt(walData.length);       // uncompressed length (for decompression)
      dos.writeInt(compressed.length);    // compressed length
      dos.write(compressed);

      dos.writeInt(bucketRecordDelta.size());
      for (final Map.Entry<Integer, Integer> entry : bucketRecordDelta.entrySet()) {
        dos.writeInt(entry.getKey());
        dos.writeInt(entry.getValue());
      }

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode TX entry", e);
    }
  }

  /**
   * Encodes a schema entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF), schemaJson length (int) and UTF-8 bytes,
   * filesToAdd map, filesToRemove map,
   * walEntries count (int), then for each WAL entry: length (int) + bytes,
   * then for each bucket delta: entry count (int) + pairs of fileId (int) and delta (int).
   * <p>
   * The schemaJson is length-prefixed rather than written via {@code writeUTF} because the
   * modified-UTF-8 format used by {@code DataOutputStream.writeUTF} is capped at 65535 bytes,
   * which realistic schemas (many types) can exceed.
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas) {
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas,
        Collections.emptyList());
  }

  /**
   * Encodes a schema entry, optionally embedding TimeSeries sealed-store blobs (issue #4382).
   * <p>
   * The sealed-blob section is appended AFTER the WAL section as a self-describing trailing section,
   * so older nodes (whose decoder stops after the WAL section) ignore it and never produce it. Each
   * blob carries its type name, shard index, file name, a CRC32 of the uncompressed bytes, and the
   * compressed bytes.
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<TsSealedBlob> sealedFileBlobs) {
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas,
        sealedFileBlobs, false);
  }

  /**
   * @param moreChunksFollow marks a non-final chunk of a schema change split across several entries
   *                         (issue #5443). Written as a trailing self-describing byte, so a node running
   *                         an older codec simply stops after the sealed-blob section and decodes it as
   *                         false - the pre-split behaviour.
   *                         <p>
   *                         <b>During a rolling upgrade</b> that means a node still running the older
   *                         codec keeps the pre-fix behaviour for split entries: it reloads its schema on
   *                         a delivery-only chunk and can detach a compacted sub-index for good, ending
   *                         up with a short index. The wire format stays compatible in both directions,
   *                         but the FIX only takes effect on a node once it is upgraded, and the symptom
   *                         is silent - fewer rows from that node, no error. Upgrade the followers, and
   *                         where a node was live through a compaction under the old codec, REBUILD INDEX
   *                         on it (or let a snapshot install replace its files) to repair what it missed.
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<TsSealedBlob> sealedFileBlobs, final boolean moreChunksFollow) {
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas,
        sealedFileBlobs, moreChunksFollow, Collections.emptyList());
  }

  /**
   * @param sealedFileChunks slices of a sealed store too large to ride one entry (issue #4416). Written as a
   *                         trailing section AFTER {@code moreChunksFollow}, which this encoder always emits, so
   *                         the flag can never be confused with the section's own first byte and a node running an
   *                         older codec stops right after the flag and decodes the entry exactly as it did before.
   *                         <p>
   *                         <b>During a rolling upgrade</b> that older node therefore installs nothing for a
   *                         SLICED sealed store while still applying the mutable-bucket clear WAL the publishing
   *                         entry carries, and serves that shard from its sealed store's previous image until it
   *                         is upgraded and resynced - the same exposure the sealed-blob section itself has had
   *                         since #4382, and for the same reason: a trailing section an old decoder cannot see. A
   *                         leader only produces slices once a sealed store outgrows one entry, so a cluster whose
   *                         stores fit inline is not exposed at all. Upgrade the followers first.
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove,
      final List<byte[]> walEntries, final List<Map<Integer, Integer>> bucketDeltas,
      final List<TsSealedBlob> sealedFileBlobs, final boolean moreChunksFollow,
      final List<TsSealedChunk> sealedFileChunks) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.SCHEMA_ENTRY.getId());
      dos.writeUTF(databaseName);
      final byte[] schemaBytes = (schemaJson != null ? schemaJson : "").getBytes(StandardCharsets.UTF_8);
      dos.writeInt(schemaBytes.length);
      dos.write(schemaBytes);

      writeFileMap(dos, filesToAdd);
      writeFileMap(dos, filesToRemove);

      final int walCount = walEntries != null ? walEntries.size() : 0;
      dos.writeInt(walCount);
      for (int i = 0; i < walCount; i++) {
        final byte[] walData = walEntries.get(i);
        checkProducedPayloadLength(walData.length, databaseName, "Schema change WAL entry " + (i + 1) + "/" + walCount);
        final byte[] compressedWal = CompressionFactory.getDefault().compress(walData);
        dos.writeInt(walData.length);         // uncompressed length
        dos.writeInt(compressedWal.length);   // compressed length
        dos.write(compressedWal);

        final Map<Integer, Integer> delta = bucketDeltas != null && i < bucketDeltas.size()
            ? bucketDeltas.get(i)
            : Collections.emptyMap();
        dos.writeInt(delta.size());
        for (final Map.Entry<Integer, Integer> e : delta.entrySet()) {
          dos.writeInt(e.getKey());
          dos.writeInt(e.getValue());
        }
      }

      // TimeSeries sealed-store blob section (trailing, backward/forward compatible).
      final int blobCount = sealedFileBlobs != null ? sealedFileBlobs.size() : 0;
      dos.writeInt(blobCount);
      for (int i = 0; i < blobCount; i++) {
        final TsSealedBlob blob = sealedFileBlobs.get(i);
        final byte[] raw = blob.bytes() != null ? blob.bytes() : new byte[0];
        checkProducedPayloadLength(raw.length, databaseName, "Sealed TimeSeries store '" + blob.fileName() + "'",
            SEALED_REMEDIATION);
        final CRC32 crc = new CRC32();
        crc.update(raw);
        final byte[] compressed = CompressionFactory.getDefault().compress(raw);
        dos.writeUTF(blob.typeName());
        dos.writeInt(blob.shardIndex());
        dos.writeUTF(blob.fileName());
        dos.writeLong(crc.getValue());
        dos.writeInt(raw.length);          // uncompressed length
        dos.writeInt(compressed.length);   // compressed length
        dos.write(compressed);
      }

      // KEEP THIS BEFORE ANY LATER SECTION, unconditionally. The decoder detects it with available() > 0, so
      // it must be written by every encoder that writes anything after it: an entry that OMITTED it and then
      // wrote a section would make that section's first byte indistinguishable from the flag. It is written
      // unconditionally here, which is precisely what lets the sealed-slice section below follow it (#4416),
      // and an older decoder - which stops here - still reads the flag correctly.
      dos.writeBoolean(moreChunksFollow);

      // TimeSeries sealed-store SLICE section (issue #4416). Trailing, self-describing, and written only when
      // non-empty, so an entry with nothing to slice is byte-identical to what the previous codec produced.
      final int chunkCount = sealedFileChunks != null ? sealedFileChunks.size() : 0;
      if (chunkCount > 0) {
        dos.writeInt(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
          final TsSealedChunk chunk = sealedFileChunks.get(i);
          final byte[] raw = chunk.bytes() != null ? chunk.bytes() : new byte[0];
          checkProducedPayloadLength(raw.length, databaseName,
              "Sealed TimeSeries store slice '" + chunk.fileName() + "'", SEALED_REMEDIATION);
          final CRC32 crc = new CRC32();
          crc.update(raw);
          final byte[] compressed = CompressionFactory.getDefault().compress(raw);
          dos.writeUTF(chunk.typeName());
          dos.writeInt(chunk.shardIndex());
          dos.writeUTF(chunk.fileName());
          dos.writeLong(chunk.fileLength());
          dos.writeLong(chunk.fileCrc());
          dos.writeLong(chunk.offset());
          dos.writeBoolean(chunk.last());
          dos.writeLong(crc.getValue());     // CRC of THIS slice
          dos.writeInt(raw.length);          // uncompressed length
          dos.writeInt(compressed.length);   // compressed length
          dos.write(compressed);
        }
      }

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode SCHEMA entry", e);
    }
  }

  /**
   * Convenience overload with no embedded WAL entries (for schema-only changes).
   */
  public static ByteString encodeSchemaEntry(final String databaseName, final String schemaJson,
      final Map<Integer, String> filesToAdd, final Map<Integer, String> filesToRemove) {
    return encodeSchemaEntry(databaseName, schemaJson, filesToAdd, filesToRemove, Collections.emptyList(), Collections.emptyList());
  }

  /**
   * Encodes an install-database entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF), forceSnapshot (boolean).
   */
  public static ByteString encodeInstallDatabaseEntry(final String databaseName) {
    return encodeInstallDatabaseEntry(databaseName, false);
  }

  /**
   * Encodes an install-database entry with an explicit forceSnapshot flag.
   * When {@code forceSnapshot} is true, replicas pull a fresh snapshot from the
   * leader even if the database already exists locally (used for restore).
   */
  public static ByteString encodeInstallDatabaseEntry(final String databaseName, final boolean forceSnapshot) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.INSTALL_DATABASE_ENTRY.getId());
      dos.writeUTF(databaseName);
      dos.writeBoolean(forceSnapshot);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode INSTALL_DATABASE entry", e);
    }
  }

  /**
   * Encodes a security-users entry into a ByteString.
   * <p>
   * Binary format: type byte, empty databaseName (UTF), jsonLength (int), UTF-8 bytes.
   * The empty databaseName slot keeps the decoder symmetric with other entry types.
   */
  public static ByteString encodeSecurityUsersEntry(final String usersJson) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.SECURITY_USERS_ENTRY.getId());
      dos.writeUTF("");
      final byte[] bytes = usersJson.getBytes(StandardCharsets.UTF_8);
      dos.writeInt(bytes.length);
      dos.write(bytes);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode SECURITY_USERS entry", e);
    }
  }

  /**
   * Encodes a bootstrap-fingerprint entry into a ByteString. Issue #4147.
   * <p>
   * Binary format: type byte, databaseName (UTF), fingerprint (UTF-8 hex), lastTxId (long).
   * Committed once per database during first cluster formation when
   * {@code arcadedb.ha.bootstrapFromLocalDatabase} is enabled, naming the peer chosen as the
   * bootstrap source. Followers verify their local fingerprint against this entry; match means
   * "bootstrap locally", mismatch means "fall back to leader-shipped snapshot".
   */
  public static ByteString encodeBootstrapFingerprintEntry(final String databaseName, final String fingerprint,
      final long lastTxId) {
    if (databaseName == null)
      throw new IllegalArgumentException("databaseName is required");
    if (fingerprint == null)
      throw new IllegalArgumentException("fingerprint is required");
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY.getId());
      dos.writeUTF(databaseName);
      final byte[] fpBytes = fingerprint.getBytes(StandardCharsets.UTF_8);
      dos.writeInt(fpBytes.length);
      dos.write(fpBytes);
      dos.writeLong(lastTxId);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode BOOTSTRAP_FINGERPRINT entry", e);
    }
  }

  /**
   * Encodes a drop-database entry into a ByteString.
   * <p>
   * Binary format: type byte, databaseName (UTF).
   */
  public static ByteString encodeDropDatabaseEntry(final String databaseName) {
    try {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();
      final DataOutputStream dos = new DataOutputStream(baos);

      dos.writeByte(RaftLogEntryType.DROP_DATABASE_ENTRY.getId());
      dos.writeUTF(databaseName);

      dos.flush();
      return ByteString.copyFrom(baos.toByteArray());
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to encode DROP_DATABASE entry", e);
    }
  }

  /**
   * Decodes a ByteString back into a DecodedEntry.
   */
  public static DecodedEntry decode(final ByteString data) {
    try (final InputStream input = data.newInput();
        final DataInputStream dis = new DataInputStream(input)) {

      final byte typeByte = dis.readByte();
      final RaftLogEntryType type = RaftLogEntryType.fromId(typeByte);
      if (type == null)
        return new DecodedEntry(null, null, null, null, null, null, null, null, null, null, false, null, -1L,
            Collections.emptyList(), false, Collections.emptyList());
      final String databaseName = dis.readUTF();

      try {
        return decodeBody(dis, type, databaseName);
      } catch (final RuntimeException | IOException e) {
        // Wrap so the caller can tell "this entry cannot be read" apart from "applying it went wrong", and can
        // see WHICH database the unreadable entry targeted - with that, one bad entry quarantines one database
        // instead of halting the whole node (issue #7138).
        throw new RaftLogEntryDecodeException(
            "Failed to decode Raft log entry of type " + type
                + (databaseName == null || databaseName.isEmpty() ? "" : " for database '" + databaseName + "'")
                + ": " + e.getMessage(), type, databaseName, e);
      }
    } catch (final IOException e) {
      throw new IllegalStateException("Failed to decode Raft log entry", e);
    }
  }

  /** Decodes everything after the {@code (type, databaseName)} envelope header. */
  private static DecodedEntry decodeBody(final DataInputStream dis, final RaftLogEntryType type,
      final String databaseName) throws IOException {
    final DecodedEntry result = switch (type) {
      case TX_ENTRY -> decodeTxEntry(dis, databaseName);
      case SCHEMA_ENTRY -> decodeSchemaEntry(dis, databaseName);
      case INSTALL_DATABASE_ENTRY -> decodeInstallDatabaseEntry(dis, databaseName);
      case DROP_DATABASE_ENTRY -> new DecodedEntry(RaftLogEntryType.DROP_DATABASE_ENTRY, databaseName,
          null, null, null, null, null, null, null, null, false, null, -1L, Collections.emptyList(), false,
          Collections.emptyList());
      case SECURITY_USERS_ENTRY -> decodeSecurityUsersEntry(dis);
      case BOOTSTRAP_FINGERPRINT_ENTRY -> decodeBootstrapFingerprintEntry(dis, databaseName);
    };

    skipTrailingExtensionSections(dis, type);

    return result;
  }

  /**
   * Consumes whatever follows the fields this version knows about, refusing anything that is not a well-formed
   * {@link #EXTENSION_MAGIC} frame (issue #7138).
   * <p>
   * This replaces a flat "any trailing byte is corruption" rule that made every non-schema type impossible to
   * extend without halting older peers, and it keeps the corruption signal that rule existed for: a truncated
   * entry ends mid-field or mid-frame, and neither carries the magic at a frame boundary.
   * <p>
   * {@code SCHEMA_ENTRY} is excluded: its optional trailing sections (the #4382 WAL blobs, the #5443 flag, the
   * #4416 slices) predate this framing and are unframed, so requiring the magic there would reject entries peers
   * legitimately produce today - and its decoder has already consumed anything that follows the file maps as one
   * of those sections, so there is nothing left here to frame. It extends through its own mechanism instead; see
   * {@link #EXTENSION_MAGIC}.
   */
  private static void skipTrailingExtensionSections(final DataInputStream dis, final RaftLogEntryType type)
      throws IOException {
    if (type == RaftLogEntryType.SCHEMA_ENTRY)
      return;

    while (dis.available() > 0) {
      if (dis.available() < EXTENSION_HEADER_BYTES)
        throw new IllegalStateException("Corrupted Raft log entry: " + dis.available()
            + " trailing bytes after " + type + " decode, too few for an extension section header");

      final int magic = dis.readInt();
      if (magic != EXTENSION_MAGIC)
        throw new IllegalStateException("Corrupted Raft log entry: trailing bytes after " + type
            + " decode are not an extension section (expected magic 0x" + Integer.toHexString(EXTENSION_MAGIC)
            + ", found 0x" + Integer.toHexString(magic) + ")");

      final int length = dis.readInt();
      checkByteLength(length, dis.available(), type + " extension section");
      dis.skipNBytes(length);
    }
  }

  private static DecodedEntry decodeTxEntry(final DataInputStream dis, final String databaseName) throws IOException {
    final int uncompressedLength = dis.readInt();
    final int compressedLength = dis.readInt();
    checkByteLength(compressedLength, dis.available(), "TX_ENTRY compressed WAL");
    checkDecompressedLength(uncompressedLength, compressedLength, "TX_ENTRY uncompressed WAL");
    final byte[] compressed = new byte[compressedLength];
    dis.readFully(compressed);
    final byte[] walData = CompressionFactory.getDefault().decompress(compressed, uncompressedLength);

    final int deltaCount = dis.readInt();
    checkCollectionSize(deltaCount, "TX_ENTRY bucket deltas");
    final Map<Integer, Integer> bucketRecordDelta = HashMap.newHashMap(deltaCount);
    for (int i = 0; i < deltaCount; i++) {
      final int bucketId = dis.readInt();
      final int delta = dis.readInt();
      bucketRecordDelta.put(bucketId, delta);
    }

    return new DecodedEntry(RaftLogEntryType.TX_ENTRY, databaseName, walData, bucketRecordDelta,
        null, null, null, null, null, null, false, null, -1L, Collections.emptyList(), false, Collections.emptyList());
  }

  private static DecodedEntry decodeSchemaEntry(final DataInputStream dis, final String databaseName) throws IOException {
    final int schemaLen = dis.readInt();
    checkByteLength(schemaLen, dis.available(), "SCHEMA_ENTRY schemaJson");
    final byte[] schemaBytes = new byte[schemaLen];
    dis.readFully(schemaBytes);
    final String schemaJson = new String(schemaBytes, StandardCharsets.UTF_8);
    final Map<Integer, String> filesToAdd = readFileMap(dis);
    final Map<Integer, String> filesToRemove = readFileMap(dis);

    // Read embedded WAL entries. The section is optional: log entries produced by nodes that predate
    // it end the stream cleanly right after the file maps. A clean section boundary leaves no bytes
    // (available()==0) and is decoded as an absent (empty) section, mirroring decodeInstallDatabaseEntry.
    // Once any bytes remain the section IS present, so a truncated/misaligned section makes the reads
    // below hit EOF and propagate as corruption rather than silently yielding empty/partial WAL pages
    // (which would apply a schema change with missing index/WAL pages on followers).
    List<byte[]> walEntries = Collections.emptyList();
    List<Map<Integer, Integer>> bucketDeltas = Collections.emptyList();
    if (dis.available() > 0) {
      final int walCount = dis.readInt();
      checkCollectionSize(walCount, "SCHEMA_ENTRY WAL entries");
      if (walCount > 0) {
        walEntries = new ArrayList<>(walCount);
        bucketDeltas = new ArrayList<>(walCount);
        for (int i = 0; i < walCount; i++) {
          final int walUncompressedLen = dis.readInt();
          final int walCompressedLen = dis.readInt();
          checkByteLength(walCompressedLen, dis.available(), "SCHEMA_ENTRY WAL compressed");
          checkDecompressedLength(walUncompressedLen, walCompressedLen, "SCHEMA_ENTRY WAL uncompressed");
          final byte[] walCompressed = new byte[walCompressedLen];
          dis.readFully(walCompressed);
          final byte[] walData = CompressionFactory.getDefault().decompress(walCompressed, walUncompressedLen);
          walEntries.add(walData);

          final int deltaCount = dis.readInt();
          checkCollectionSize(deltaCount, "SCHEMA_ENTRY bucket deltas");
          final Map<Integer, Integer> delta = HashMap.newHashMap(deltaCount);
          for (int j = 0; j < deltaCount; j++)
            delta.put(dis.readInt(), dis.readInt());
          bucketDeltas.add(delta);
        }
      }
    }

    // TimeSeries sealed-store blob section (issue #4382). Trailing, self-describing section with the
    // same presence rule as the WAL section above: no remaining bytes (available()==0) means the
    // section is absent (older entry) and is decoded as empty. Once any bytes remain the section IS
    // present, so a truncated/misaligned blob makes the reads below hit EOF and propagate rather than
    // being silently dropped; a CRC mismatch on a fully-read blob is likewise a hard failure.
    List<TsSealedBlob> sealedFileBlobs = Collections.emptyList();
    if (dis.available() > 0) {
      final int blobCount = dis.readInt();
      checkCollectionSize(blobCount, "SCHEMA_ENTRY sealed blobs");
      if (blobCount > 0) {
        sealedFileBlobs = new ArrayList<>(blobCount);
        for (int i = 0; i < blobCount; i++) {
          final String typeName = dis.readUTF();
          final int shardIndex = dis.readInt();
          final String fileName = dis.readUTF();
          final long expectedCrc = dis.readLong();
          final int uncompressedLen = dis.readInt();
          final int compressedLen = dis.readInt();
          checkByteLength(compressedLen, dis.available(), "SCHEMA_ENTRY sealed blob compressed");
          checkDecompressedLength(uncompressedLen, compressedLen, "SCHEMA_ENTRY sealed blob uncompressed");
          final byte[] compressed = new byte[compressedLen];
          dis.readFully(compressed);
          final byte[] raw = CompressionFactory.getDefault().decompress(compressed, uncompressedLen);
          final CRC32 crc = new CRC32();
          crc.update(raw);
          if (crc.getValue() != expectedCrc)
            throw new IllegalStateException(
                "CRC mismatch decoding SCHEMA_ENTRY sealed blob for '" + fileName + "' (corrupted replication payload)");
          sealedFileBlobs.add(new TsSealedBlob(typeName, shardIndex, fileName, raw));
        }
      }
    }

    // Trailing continuation flag (issue #5443), same presence rule as the sections above: absent on
    // entries produced by an older codec, which decode as false.
    final boolean moreChunksFollow = dis.available() > 0 && dis.readBoolean();

    // TimeSeries sealed-store SLICE section (issue #4416). It can only be read once the flag above has been
    // consumed, which is why that flag is written unconditionally by the encoder; the same presence rule applies
    // here, and a truncated slice makes the reads below hit EOF and propagate rather than being silently dropped.
    List<TsSealedChunk> sealedFileChunks = Collections.emptyList();
    if (dis.available() > 0) {
      final int chunkCount = dis.readInt();
      checkCollectionSize(chunkCount, "SCHEMA_ENTRY sealed slices");
      if (chunkCount > 0) {
        sealedFileChunks = new ArrayList<>(chunkCount);
        for (int i = 0; i < chunkCount; i++) {
          final String typeName = dis.readUTF();
          final int shardIndex = dis.readInt();
          final String fileName = dis.readUTF();
          final long fileLength = dis.readLong();
          final long fileCrc = dis.readLong();
          final long offset = dis.readLong();
          final boolean last = dis.readBoolean();
          final long expectedCrc = dis.readLong();
          final int uncompressedLen = dis.readInt();
          final int compressedLen = dis.readInt();
          checkByteLength(compressedLen, dis.available(), "SCHEMA_ENTRY sealed slice compressed");
          checkDecompressedLength(uncompressedLen, compressedLen, "SCHEMA_ENTRY sealed slice uncompressed");
          final byte[] compressed = new byte[compressedLen];
          dis.readFully(compressed);
          final byte[] raw = CompressionFactory.getDefault().decompress(compressed, uncompressedLen);
          final CRC32 crc = new CRC32();
          crc.update(raw);
          if (crc.getValue() != expectedCrc)
            throw new IllegalStateException("CRC mismatch decoding SCHEMA_ENTRY sealed slice for '" + fileName
                + "' at offset " + offset + " (corrupted replication payload)");
          checkSealedSliceGeometry(fileName, fileLength, offset, raw.length, last);
          sealedFileChunks.add(
              new TsSealedChunk(typeName, shardIndex, fileName, fileLength, fileCrc, offset, raw, last));
        }
      }
    }

    return new DecodedEntry(RaftLogEntryType.SCHEMA_ENTRY, databaseName, null, null,
        schemaJson, filesToAdd, filesToRemove, walEntries, bucketDeltas, null, false, null, -1L, sealedFileBlobs,
        moreChunksFollow, sealedFileChunks);
  }

  private static DecodedEntry decodeInstallDatabaseEntry(final DataInputStream dis, final String databaseName) throws IOException {
    // Length-based detection of the trailing forceSnapshot flag.
    // Legacy entries (pre-forceSnapshot codec) have no trailing byte; they decode as forceSnapshot=false.
    boolean forceSnapshot = false;
    if (dis.available() > 0) {
      forceSnapshot = dis.readBoolean();
    }
    return new DecodedEntry(RaftLogEntryType.INSTALL_DATABASE_ENTRY, databaseName,
        null, null, null, null, null, null, null, null, forceSnapshot, null, -1L, Collections.emptyList(), false,
        Collections.emptyList());
  }

  private static DecodedEntry decodeBootstrapFingerprintEntry(final DataInputStream dis, final String databaseName)
      throws IOException {
    final int fpLen = dis.readInt();
    checkByteLength(fpLen, dis.available(), "BOOTSTRAP_FINGERPRINT fingerprint");
    final byte[] fpBytes = new byte[fpLen];
    dis.readFully(fpBytes);
    final String fingerprint = new String(fpBytes, StandardCharsets.UTF_8);
    final long lastTxId = dis.readLong();
    return new DecodedEntry(RaftLogEntryType.BOOTSTRAP_FINGERPRINT_ENTRY, databaseName,
        null, null, null, null, null, null, null, null, false, fingerprint, lastTxId, Collections.emptyList(), false,
        Collections.emptyList());
  }

  private static DecodedEntry decodeSecurityUsersEntry(final DataInputStream dis) throws IOException {
    final int length = dis.readInt();
    checkByteLength(length, dis.available(), "SECURITY_USERS_ENTRY");
    final byte[] bytes = new byte[length];
    dis.readFully(bytes);
    final String usersJson = new String(bytes, StandardCharsets.UTF_8);
    return new DecodedEntry(RaftLogEntryType.SECURITY_USERS_ENTRY, "",
        null, null, null, null, null, null, null, usersJson, false, null, -1L, Collections.emptyList(), false,
        Collections.emptyList());
  }

  private static void writeFileMap(final DataOutputStream dos, final Map<Integer, String> fileMap) throws IOException {
    if (fileMap == null) {
      dos.writeInt(0);
      return;
    }
    dos.writeInt(fileMap.size());
    for (final Map.Entry<Integer, String> entry : fileMap.entrySet()) {
      dos.writeInt(entry.getKey());
      final String value = entry.getValue();
      final boolean hasValue = value != null;
      dos.writeBoolean(hasValue);
      if (hasValue)
        dos.writeUTF(value);
    }
  }

  private static Map<Integer, String> readFileMap(final DataInputStream dis) throws IOException {
    final int count = dis.readInt();
    checkCollectionSize(count, "file map");
    final Map<Integer, String> map = HashMap.newHashMap(count);
    for (int i = 0; i < count; i++) {
      final int fileId = dis.readInt();
      final boolean hasValue = dis.readBoolean();
      final String fileName = hasValue ? dis.readUTF() : null;
      map.put(fileId, fileName);
    }
    return map;
  }
}
