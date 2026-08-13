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
package com.arcadedb.engine;

import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexInternal;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;
import com.arcadedb.schema.LocalSchema;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * Lists the paginated files a node holds that nothing in its schema claims (issue #6143).
 * <p>
 * WHAT LEAVES THEM BEHIND. A schema session that ships its WAL in instalments announces the files it creates before
 * the change is published, and retires them itself if it never gets there. It cannot do that once it has lost
 * leadership mid-session - a node that is no longer leader cannot make the cluster do anything - so the files stay on
 * the other nodes, referenced by nothing. The failing node logs them at SEVERE, but the nodes actually HOLDING them
 * log nothing at all, and an operator could previously only find them by reading the data directory by hand. The same
 * shape is left on ANY node by a DDL that throws after creating a file without publishing it.
 * <p>
 * Nothing reads these files: no query, index or replication path resolves a file no schema component references, so
 * the impact is wasted disk. This pass therefore only REPORTS. It never deletes: a file whose reference this walk
 * simply does not know how to follow would be data, and taking that risk to reclaim disk is the wrong trade. What to
 * do about a finding is an operator decision, taken with the node stopped.
 * <p>
 * <b>The three shapes it can prove</b>, and it deliberately proves nothing else:
 * <ol>
 *   <li>a file the {@link FileManager} holds with NO schema component at all. This is what an abandoned instalment
 *       leaves on a follower while it runs: the follower's apply path creates the file and only a later schema
 *       reload would turn it into a component;</li>
 *   <li>a {@link LocalBucket} component no type lists among its buckets and no type pairs to one of its buckets as
 *       an external-property bucket. This is what the same file becomes after the next schema reload, and what a
 *       failed {@code CREATE BUCKET} leaves on the node that ran it;</li>
 *   <li>an automatic index whose files no type references - what an abandoned index rebuild leaves.</li>
 * </ol>
 * Every other component is treated as CLAIMED without checking: the dictionary, a compacted sub-index, a bloom
 * filter, a vector index's companion graph file, the time-series internals. Each of those is referenced by another
 * component rather than by the schema JSON, through a link this walk does not follow, so reporting them would be a
 * false positive on a healthy database - the one outcome that would make this diagnostic worse than useless.
 * Manual indexes are claimed for the same reason: they are bound to no type by design. So is any bucket whose NAME
 * is derived from a type's or a claimed bucket's - the edge-list buckets of a vertex bucket, a super-node stripe
 * pool, a paired external bucket - see {@link #isDerivedFromAnOwner}, which recognises the convention rather than
 * the individual features that follow it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class UnreferencedFiles {

  /**
   * One file this node holds that nothing claims.
   *
   * @param fileId   the file id, which is what a removal or a support request has to name
   * @param fileName the file name on disk, relative to the database directory
   * @param reason   which of the three shapes above this is, phrased for an operator
   */
  public record UnreferencedFile(int fileId, String fileName, String reason) {
    @Override
    public String toString() {
      return fileName + " (fileId=" + fileId + ", " + reason + ")";
    }
  }

  private UnreferencedFiles() {
  }

  /**
   * Walks this node's files and returns those nothing claims, ordered by file id.
   * <p>
   * O(files + buckets + indexes) with no I/O: it reads the in-memory registries only, so it is safe to call on a
   * live database and cheap enough for a metrics refresh. It takes no lock beyond the snapshots the registries hand
   * out, so a file created or dropped WHILE it runs may be reported or missed - a diagnostic reading a moving state
   * either way. A finding worth acting on is one that survives a second call.
   */
  public static List<UnreferencedFile> scan(final DatabaseInternal database) {
    // Ordered by file id so two calls on an unchanged database produce identical output, which is what makes
    // "did my repair work?" answerable by comparing them.
    final TreeMap<Integer, UnreferencedFile> found = new TreeMap<>();

    walk(database, (fileId, file, reason) -> found.put(fileId,
        new UnreferencedFile(fileId, file.getFileName(), reason.get())));

    return new ArrayList<>(found.values());
  }

  /**
   * How many files nothing claims, for callers that publish only a number - the per-database HA gauge, refreshed on
   * a timer.
   * <p>
   * Shares {@link #walk} with {@link #scan} rather than counting its result, so the descriptive reason of each
   * finding is never built here: it is a {@code Supplier} this consumer simply does not call. Free in the common
   * case, where there is nothing to describe, and it keeps a node that IS leaking files from paying for strings
   * every refresh throws away.
   * <p>
   * The walk itself is NOT memoized, and the whole of it - including rebuilding the claimed-file set - runs on every
   * refresh. That is deliberate for a diagnostic reading live state, and it is in-memory work with no I/O, but it
   * does scale with the schema: one {@code getFileIds()} per index, each taking that index's read lock. Should a
   * database with tens of thousands of components ever make that visible, the gate is
   * {@code (FileManager.getModificationCount(), schema version)} - between them those two cover every way the answer
   * can change, since a file this walk would newly report appeared either as a file (the first) or as a schema
   * change that stopped claiming one (the second). Recorded rather than implemented: a cached count that goes stale
   * is a worse diagnostic than a slightly expensive one.
   */
  public static long count(final DatabaseInternal database) {
    final long[] count = { 0 };
    walk(database, (fileId, file, reason) -> ++count[0]);
    return count[0];
  }

  /**
   * The walk itself, emitting each unclaimed file exactly once, in file-id order.
   * <p>
   * The reason is passed as a {@link Supplier} so a consumer that only counts never builds it.
   */
  private static void walk(final DatabaseInternal database, final UnreferencedFileConsumer consumer) {
    final LocalSchema localSchema = database.getSchema().getEmbedded();
    // Type and claimed-bucket names, for the derived-name rule below.
    final Set<String> owners = new HashSet<>();
    final BitSet claimed = collectClaimedFileIds(localSchema, owners);
    // What has already been emitted, so the index pass below cannot report a file the file pass above did.
    final BitSet reported = new BitSet();

    final List<ComponentFile> physicalFiles = database.getFileManager().getFiles();
    for (int fileId = 0; fileId < physicalFiles.size(); fileId++) {
      final ComponentFile file = physicalFiles.get(fileId);
      if (file == null)
        continue;

      final Component component = localSchema.getFileByIdIfExists(fileId);
      if (component == null) {
        reported.set(fileId);
        consumer.accept(fileId, file,
            () -> "the file exists on this node but no schema component was ever built for it, which is the state a "
                + "replicated schema change leaves behind when it delivers a file and is then abandoned");
        continue;
      }

      if (component instanceof LocalBucket bucket && !claimed.get(fileId)
          // Purpose is restored from the schema at load time and reset to PRIMARY when it is not, so it can only be
          // used to EXCLUDE: anything not claiming to be a plain data bucket is left to whoever paired it.
          && bucket.getPurpose() == LocalBucket.Purpose.PRIMARY
          && !isDerivedFromAnOwner(bucket.getName(), owners)) {
        reported.set(fileId);
        consumer.accept(fileId, file, () -> "bucket '" + bucket.getName() + "' belongs to no type");
      }
    }

    walkUnreferencedIndexFiles(localSchema, claimed, reported, physicalFiles, consumer);
  }

  /** Receives one unclaimed file. The reason is deferred so a counting consumer never pays for it. */
  @FunctionalInterface
  private interface UnreferencedFileConsumer {
    void accept(int fileId, ComponentFile file, Supplier<String> reason);
  }

  /**
   * The file ids the schema reaches from a type: its buckets, the external-property bucket paired to each of them,
   * and the files of every index on it. Manual indexes are added too - they are bound to no type by design, so
   * leaving them out would report every manual index in the database as an orphan.
   * <p>
   * A {@link BitSet} rather than a {@code Set<Integer>}: file ids are dense small ints, so this is one word per 64
   * of them with no boxing, and the walk below queries it once per file - a linear structure would make the whole
   * scan quadratic on a schema with thousands of buckets.
   */
  private static BitSet collectClaimedFileIds(final LocalSchema schema, final Set<String> owners) {
    final BitSet claimed = new BitSet();

    for (final DocumentType type : schema.getTypes()) {
      owners.add(type.getName());

      for (final Bucket bucket : type.getBuckets(false)) {
        claim(claimed, bucket.getFileId());
        owners.add(bucket.getName());

        if (type instanceof LocalDocumentType localType) {
          final Integer externalBucketId = localType.getExternalBucketIdFor(bucket.getFileId());
          if (externalBucketId != null)
            claim(claimed, externalBucketId);
        }
      }

      for (final Index index : type.getAllIndexes(true))
        if (index instanceof IndexInternal internal)
          for (final int fileId : internal.getFileIds())
            claim(claimed, fileId);
    }

    for (final Index index : schema.getIndexes())
      if (index instanceof IndexInternal internal && !internal.isAutomatic())
        for (final int fileId : internal.getFileIds())
          claim(claimed, fileId);

    return claimed;
  }

  /**
   * Whether a bucket no type lists is nevertheless OWNED by one, because its name is derived from a type's or a
   * claimed bucket's - {@code Hub_sn_stripe_3}, {@code V_0_out_edges}, {@code Doc_0_ext}.
   * <p>
   * A GENERAL rule, and deliberately so. This started as a list of the conventions in the engine and CI found the
   * one that list was missing (the super-node stripe pools of #5156, whose buckets no type lists and which
   * {@code StripedEdgeList} reaches by composing the type name), which is the proof that enumerating them is not a
   * closed problem: every internal bucket names itself after the schema object it belongs to, and the next feature
   * to add one will follow the same convention rather than register with this class. Recognising the convention
   * itself covers them all, including the ones not written yet.
   * <p>
   * What it costs is a false NEGATIVE: a genuinely orphaned bucket that happens to be named after a surviving type
   * goes unreported. That is the right side to fail on for a diagnostic whose findings invite deletion - and the
   * shape that motivated it is not affected, since a session abandoned before it published left no type to derive
   * the name from.
   * <p>
   * Matched by successive underscore-delimited prefixes rather than by scanning the owner set, so the cost is a few
   * hash lookups per candidate instead of one pass over every type in the schema.
   */
  private static boolean isDerivedFromAnOwner(final String bucketName, final Set<String> owners) {
    for (int underscore = bucketName.indexOf('_'); underscore > 0; underscore = bucketName.indexOf('_',
        underscore + 1))
      if (owners.contains(bucketName.substring(0, underscore)))
        return true;

    return false;
  }

  /**
   * Marks one file id as claimed. Negative ids are dropped rather than allowed to throw: an index whose bucket was
   * lost reports {@code -1} for it (the orphan sub-index of issue #5780), and a diagnostic must not be the thing
   * that fails on a database that is already damaged.
   */
  private static void claim(final BitSet claimed, final int fileId) {
    if (fileId >= 0)
      claimed.set(fileId);
  }

  /**
   * Reports the files of an automatic index that no type reaches - what an abandoned index rebuild leaves behind.
   * <p>
   * Asked of the INDEX rather than of each file component, because an index owns more than one file (its mutable
   * component plus, once it has been compacted, its compacted sub-index; a vector index also owns its graph file)
   * and only the index itself can say so. Every file it names is reported, so the operator gets the whole set rather
   * than the one file whose component happened to be classifiable.
   */
  private static void walkUnreferencedIndexFiles(final LocalSchema schema, final BitSet claimed,
      final BitSet reported, final List<ComponentFile> physicalFiles, final UnreferencedFileConsumer consumer) {
    for (final Index index : schema.getIndexes()) {
      if (!(index instanceof IndexInternal internal) || !internal.isAutomatic())
        continue;

      for (final int fileId : internal.getFileIds()) {
        if (fileId < 0 || claimed.get(fileId) || reported.get(fileId))
          continue;
        if (fileId >= physicalFiles.size())
          continue;
        final ComponentFile file = physicalFiles.get(fileId);
        if (file == null)
          continue;

        reported.set(fileId);
        consumer.accept(fileId, file, () -> "index '" + index.getName() + "' is referenced by no type");
      }
    }
  }
}
