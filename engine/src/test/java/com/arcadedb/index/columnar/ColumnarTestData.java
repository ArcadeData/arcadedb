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

import com.arcadedb.database.BucketPageLayoutTestSupport;
import com.arcadedb.database.Database;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.serializer.json.JSONObject;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 * Seeded test data of the columnar projection (shared by plans 3 to 6): an {@code Asset} type with a COLUMNAR index on each of its
 * facet properties, records inserted and mutated with every random draw taken from {@code new Random(seed)}, so one seed and one set
 * of knobs always produce the same data.
 * <p>
 * Every 50th record carries one of its tags twice (an in-record duplicate counts the document once) and every 10th has no author (a
 * missing value). The filler records that seal a page for a placeholder have no facet value: they count as missing on both
 * sides of {@link FacetOracle}.
 */
public final class ColumnarTestData {
  public static final String TYPE      = "Asset";
  public static final String EDGE_TYPE = "Related";
  /** The tag vertex type of {@link #createTagTree}, keyed by {@code path STRING}. */
  public static final String TAG_TYPE    = "Tag";
  /** The edge of {@link #createTagTree}, running {@code (parent)-[ParentOf]->(child)}. */
  public static final String PARENT_EDGE = "ParentOf";

  private static final int    TRANSACTION_SIZE        = 500;
  private static final int    MUTATION_BATCH          = 100;
  private static final long   DAY_MS                  = 86_400_000L;
  private static final long   EPOCH_2020_MS           = 1_577_836_800_000L;
  private static final int    DATE_RANGE_DAYS         = 5 * 365;
  private static final int    PLACEHOLDER_ATTEMPTS    = 4;
  private static final int    PLACEHOLDER_GROWTH      = 20 * 1024;
  private static final double INSERT_SHARE_OF_UPDATES = 0.3;
  private static final double EDGE_ON_UPDATE          = 0.2;

  // knobs
  public int     brands           = 200;
  public int     tagVocabulary    = 2_000;
  public int     maxTags          = 8;
  /** Share of records whose body is padded to 5 pages, so they are chunk chains (since #6149 growth never makes a placeholder). */
  public double  bigRecordRatio   = 0.0;
  /**
   * Every {@code 1 / placeholderRatio} inserted records, between transactions, a tiny record is inserted, the page it landed on is
   * sealed ({@link BucketPageLayoutTestSupport#sealFirstPage}, the page receiving inserts of the single bucket) and the tiny record is
   * grown, which leaves a placeholder (checked: an AssertionError when the growth did not make one). Made by {@link #insertAssets}
   * only, and best used on a fresh type: once {@link #mutate} deleted records, a tiny record can land in a hole of an older page, and
   * after 4 such attempts the fixture fails. Ignored when {@code buckets > 1} and for vertices (the seal writes documents).
   */
  public double  placeholderRatio = 0.0;
  public double  deleteRatio      = 0.0;
  /** Share of mutations that delete a record and insert a new one in the next transaction, so the freed slot can be reused. */
  public double  reuseRatio       = 0.0;
  public boolean externalBody     = false;
  public boolean asVertices       = false;
  public int     edgesPerVertex   = 0;
  /** More than 1 creates the type with that many buckets. */
  public int     buckets          = 1;
  /** False makes {@link #createSchema} create the type and its properties only, so a test can add a COLUMNAR index over existing data. */
  public boolean columnarIndexes  = true;
  /** Adds {@code score DOUBLE} (in [0, 100) with 2 decimals, a COLUMNAR DOUBLE column) to the schema and to every record. */
  public boolean doubleColumn     = false;
  /** Adds {@code day DATE} (a LocalDate over the same 5 years, a COLUMNAR LONG column of epoch days) to the schema and every record. */
  public boolean dateColumn       = false;
  /** Gives {@code publishDate} a random time of day (to the millisecond) instead of midnight UTC, so it straddles zone boundaries. */
  public boolean timeOfDay        = false;
  /** The records without an author store an explicit null (and an update clears it to null) instead of leaving the property out. */
  public boolean explicitNulls    = false;
  /**
   * The {@code hierarchy} object of the METADATA {@link #createSchema} puts on the tags index (null: no hierarchy). When set, the tag
   * schema of {@link #createTagSchema} is created first, so the hierarchy validates at CREATE INDEX.
   */
  public JSONObject   tagHierarchy = null;
  /** The values the tags of inserted and updated records are drawn from (null: {@code tag-N} over {@link #tagVocabulary}). */
  public List<String> tagValues    = null;

  private final Random    random;
  private final List<RID> live = new ArrayList<>();
  private long            generated;
  private int             pendingReuseInserts;
  private int             pageSize;

  public ColumnarTestData(final long seed) {
    this.random = new Random(seed);
  }

  /**
   * Creates {@code Asset(brand STRING, tags LIST OF STRING, fileSize LONG, publishDate DATETIME, author STRING, body STRING)}, body
   * EXTERNAL when {@link #externalBody}, a vertex type when {@link #asVertices}, and (unless {@link #columnarIndexes} is false) a COLUMNAR
   * index on brand, tags BY ITEM (with {@link #tagHierarchy} as its METADATA when set), fileSize, publishDate and author, then waits
   * for this node's projection to publish every column.
   */
  public void createSchema(final Database db) {
    db.command("sql", "CREATE " + (asVertices ? "VERTEX" : "DOCUMENT") + " TYPE " + TYPE + " BUCKETS " + Math.max(1, buckets));
    db.command("sql", "CREATE PROPERTY " + TYPE + ".brand STRING");
    db.command("sql", "CREATE PROPERTY " + TYPE + ".tags LIST OF STRING");
    db.command("sql", "CREATE PROPERTY " + TYPE + ".fileSize LONG");
    db.command("sql", "CREATE PROPERTY " + TYPE + ".publishDate DATETIME");
    db.command("sql", "CREATE PROPERTY " + TYPE + ".author STRING");
    db.command("sql", "CREATE PROPERTY " + TYPE + ".body STRING" + (externalBody ? " (EXTERNAL true)" : ""));
    if (doubleColumn)
      db.command("sql", "CREATE PROPERTY " + TYPE + ".score DOUBLE");
    if (dateColumn)
      db.command("sql", "CREATE PROPERTY " + TYPE + ".day DATE");
    if (asVertices)
      db.command("sql", "CREATE EDGE TYPE " + EDGE_TYPE);
    if (!columnarIndexes)
      return;
    db.command("sql", "CREATE INDEX ON " + TYPE + " (brand) COLUMNAR");
    if (tagHierarchy != null) {
      createTagSchema(db);
      db.command("sql", "CREATE INDEX ON " + TYPE + " (tags BY ITEM) COLUMNAR METADATA " + new JSONObject().put("hierarchy", tagHierarchy));
    } else
      db.command("sql", "CREATE INDEX ON " + TYPE + " (tags BY ITEM) COLUMNAR");
    db.command("sql", "CREATE INDEX ON " + TYPE + " (fileSize) COLUMNAR");
    db.command("sql", "CREATE INDEX ON " + TYPE + " (publishDate) COLUMNAR");
    db.command("sql", "CREATE INDEX ON " + TYPE + " (author) COLUMNAR");
    if (doubleColumn)
      db.command("sql", "CREATE INDEX ON " + TYPE + " (score) COLUMNAR");
    if (dateColumn)
      db.command("sql", "CREATE INDEX ON " + TYPE + " (day) COLUMNAR");
    // the columns of an empty bucket are published by background builds: none may still run once records exist, or it would move
    // rows a test expects in the overlay into a segment, and remark pages while a test holds the apply
    FacetOracle.awaitCaughtUp(db);
  }

  /** Creates, when absent, {@code Tag(path STRING)} with a NOTUNIQUE index on {@code path} and the edge type {@code ParentOf}. */
  public static void createTagSchema(final Database db) {
    db.command("sql", "CREATE VERTEX TYPE " + TAG_TYPE + " IF NOT EXISTS");
    db.command("sql", "CREATE PROPERTY " + TAG_TYPE + ".path IF NOT EXISTS STRING");
    db.command("sql", "CREATE INDEX IF NOT EXISTS ON " + TAG_TYPE + " (path) NOTUNIQUE");
    db.command("sql", "CREATE EDGE TYPE " + PARENT_EDGE + " IF NOT EXISTS");
  }

  /**
   * Creates (in one transaction, after {@link #createTagSchema}) a tag forest of {@code fanout} roots {@code t0..t<fanout-1>}, each
   * node with {@code fanout} children {@code <parent>/t<i>}, {@code depth} levels in all, linked by {@code ParentOf} edges from parent
   * to child. The seed shuffles the order the vertices and edges are created in, never the tree.
   *
   * @return every path, in creation order
   */
  public static List<String> createTagTree(final Database db, final long seed, final int depth, final int fanout) {
    createTagSchema(db);
    final List<String> paths = new ArrayList<>();
    final List<Integer> parentOf = new ArrayList<>();
    int levelStart = 0;
    for (int level = 0; level < depth; level++) {
      final int levelEnd = paths.size();
      if (level == 0) {
        for (int i = 0; i < fanout; i++) {
          paths.add("t" + i);
          parentOf.add(-1);
        }
      } else
        for (int parent = levelStart; parent < levelEnd; parent++)
          for (int i = 0; i < fanout; i++) {
            paths.add(paths.get(parent) + "/t" + i);
            parentOf.add(parent);
          }
      levelStart = levelEnd;
    }

    final Random random = new Random(seed);
    final List<Integer> order = new ArrayList<>(paths.size());
    for (int i = 0; i < paths.size(); i++)
      order.add(i);
    Collections.shuffle(order, random);
    final List<Integer> edgeOrder = new ArrayList<>(order);
    Collections.shuffle(edgeOrder, random);

    final MutableVertex[] vertices = new MutableVertex[paths.size()];
    final List<String> created = new ArrayList<>(paths.size());
    db.transaction(() -> {
      for (final int node : order) {
        vertices[node] = db.newVertex(TAG_TYPE).set("path", paths.get(node)).save();
        created.add(paths.get(node));
      }
      for (final int node : edgeOrder)
        if (parentOf.get(node) >= 0)
          vertices[parentOf.get(node)].newEdge(PARENT_EDGE, vertices[node]);
    });
    return created;
  }

  /**
   * Inserts {@code count} records in transactions of 500 (shorter ones around a placeholder) and returns the RIDs of those drawn
   * records only: the placeholder records and the fillers that seal their pages are not in the list, so with {@link #placeholderRatio}
   * the type holds more records than the list.
   */
  public List<RID> insertAssets(final Database db, final int count) {
    final List<RID> inserted = new ArrayList<>(count);
    final int placeholderEvery = placeholderRatio > 0 && buckets <= 1 && !asVertices ? Math.max(1, (int) Math.round(1 / placeholderRatio))
        : 0;
    int done = 0;
    while (done < count) {
      int end = Math.min(done + TRANSACTION_SIZE, count);
      if (placeholderEvery > 0)
        end = Math.min(end, (done / placeholderEvery + 1) * placeholderEvery);
      final int from = done;
      final int to = end;
      db.transaction(() -> {
        for (int i = from; i < to; i++)
          inserted.add(insert(db));
      });
      done = end;
      if (placeholderEvery > 0 && done % placeholderEvery == 0)
        makePlaceholder(db);
    }
    return inserted;
  }

  /**
   * Applies {@code operations} random mutations, by the knobs, in transactions of 100: deletes ({@link #deleteRatio}), deletes whose
   * slot a later insert can reuse ({@link #reuseRatio}), inserts, and updates of one property (a body update grows or shrinks the record
   * in its page, or into and out of a chunk chain when {@link #bigRecordRatio} is set; a vertex update may add an edge).
   */
  public void mutate(final Database db, final int operations) {
    int done = 0;
    while (done < operations) {
      final int batch = Math.min(MUTATION_BATCH, operations - done);
      db.transaction(() -> {
        for (; pendingReuseInserts > 0; pendingReuseInserts--)
          insert(db);
        for (int i = 0; i < batch; i++)
          mutateOne(db);
      });
      done += batch;
    }
    if (pendingReuseInserts > 0)
      db.transaction(() -> {
        for (; pendingReuseInserts > 0; pendingReuseInserts--)
          insert(db);
      });
  }

  private void mutateOne(final Database db) {
    final double operation = random.nextDouble();
    if (live.isEmpty()) {
      insert(db);
    } else if (operation < deleteRatio) {
      delete(db);
    } else if (operation < deleteRatio + reuseRatio) {
      delete(db);
      pendingReuseInserts++;
    } else if (random.nextDouble() < INSERT_SHARE_OF_UPDATES) {
      insert(db);
    } else
      update(db, live.get(random.nextInt(live.size())));
  }

  private RID insert(final Database db) {
    final MutableDocument record = asVertices ? db.newVertex(TYPE) : db.newDocument(TYPE);
    fill(db, record, generated++);
    record.save();
    if (asVertices)
      for (int i = 0; i < edgesPerVertex && !live.isEmpty(); i++)
        ((MutableVertex) record).newEdge(EDGE_TYPE, live.get(random.nextInt(live.size())));
    final RID rid = record.getIdentity();
    live.add(rid);
    return rid;
  }

  private void fill(final Database db, final MutableDocument record, final long sequence) {
    record.set("brand", brand());
    record.set("tags", tags(sequence % 50 == 0));
    record.set("fileSize", fileSize());
    record.set("publishDate", publishDate());
    final String author = author();
    if (sequence % 10 != 0)
      record.set("author", author);
    else if (explicitNulls)
      record.set("author", null);
    record.set("body", body(db, random.nextDouble() < bigRecordRatio));
    // drawn after every default value, so a test that leaves these knobs off keeps its seeded data
    if (doubleColumn)
      record.set("score", random.nextInt(10_000) / 100d);
    if (dateColumn)
      record.set("day", LocalDate.ofEpochDay(EPOCH_2020_MS / DAY_MS + random.nextInt(DATE_RANGE_DAYS)));
  }

  private void update(final Database db, final RID rid) {
    // through db, not the RID: a RID keeps the database it was created on, closed once that node stopped (a failover test)
    final MutableDocument record = db.lookupByRID(rid, true).asDocument().modify();
    switch (random.nextInt(5)) {
    case 0 -> record.set("brand", brand());
    case 1 -> record.set("tags", tags(random.nextInt(50) == 0));
    case 2 -> record.set("fileSize", fileSize());
    case 3 -> {
      final String author = author();
      if (random.nextBoolean()) {
        if (explicitNulls)
          record.set("author", null);
        else
          record.remove("author");
      } else
        record.set("author", author);
    }
    default -> record.set("body", body(db, bigRecordRatio > 0 && random.nextBoolean()));
    }
    record.save();
    if (asVertices && edgesPerVertex > 0 && random.nextDouble() < EDGE_ON_UPDATE)
      ((MutableVertex) record).newEdge(EDGE_TYPE, live.get(random.nextInt(live.size())));
  }

  private void delete(final Database db) {
    final int index = random.nextInt(live.size());
    final RID rid = live.get(index);
    live.set(index, live.getLast());
    live.removeLast();
    db.deleteRecord(db.lookupByRID(rid, true));
  }

  /**
   * Inserts a tiny record, seals the page it landed on and grows the record: with no free tail on the page and a slot too small for a
   * chunk header, the growth leaves a placeholder (#6149). A tiny record that landed on another page than the sealed one is retried
   * (it keeps its 1-character brand, a real value on both oracle sides).
   */
  private void makePlaceholder(final Database db) {
    final String brand = brand();
    final String padding = "p".repeat(PLACEHOLDER_GROWTH);
    for (int attempt = 0; attempt < PLACEHOLDER_ATTEMPTS; attempt++) {
      final RID[] tiny = new RID[1];
      // big enough for the 9-byte placeholder pointer, too small for a chunk header (an empty document is smaller than the pointer)
      db.transaction(() -> tiny[0] = db.newDocument(TYPE).set("brand", "t").save().getIdentity());
      // sealFirstPage's own check is cumulative over the bucket: prove THIS seal spilled
      final long chunkedBefore = layoutCount(db, "totalMultiPageRecords");
      final RID seal = BucketPageLayoutTestSupport.sealFirstPage(db, TYPE);
      if (layoutCount(db, "totalMultiPageRecords") <= chunkedBefore)
        throw new AssertionError("Sealing the page of " + seal + " did not spill its last record into chunks");
      final int maxRecordsInPage = bucket(db).getMaxRecordsInPage();
      if (tiny[0].getPosition() / maxRecordsInPage == seal.getPosition() / maxRecordsInPage) {
        final long placeholdersBefore = layoutCount(db, "totalPlaceholderRecords");
        db.transaction(() -> db.lookupByRID(tiny[0], true).asDocument().modify().set("brand", brand).set("v", padding).save());
        if (layoutCount(db, "totalPlaceholderRecords") != placeholdersBefore + 1)
          throw new AssertionError("Growing " + tiny[0] + " on its sealed page did not leave a placeholder");
        live.add(tiny[0]);
        return;
      }
    }
    throw new AssertionError("No tiny record landed on a sealed page in " + PLACEHOLDER_ATTEMPTS + " attempts");
  }

  private static long layoutCount(final Database db, final String name) {
    return (Long) BucketPageLayoutTestSupport.bucketStats(db, TYPE).get(name);
  }

  private String brand() {
    return "brand-" + random.nextInt(brands);
  }

  private List<String> tags(final boolean withDuplicate) {
    final int count = random.nextInt(maxTags + 1);
    final List<String> tags = new ArrayList<>(count + 2);
    for (int i = 0; i < count; i++)
      tags.add(tag());
    if (withDuplicate) {
      if (tags.isEmpty())
        tags.add(tag());
      tags.add(tags.get(random.nextInt(tags.size())));
    }
    return tags;
  }

  private String tag() {
    return tagValues != null ? tagValues.get(random.nextInt(tagValues.size())) : "tag-" + random.nextInt(tagVocabulary);
  }

  private long fileSize() {
    return 1024L * random.nextInt(4096);
  }

  private Date publishDate() {
    final long day = EPOCH_2020_MS + random.nextInt(DATE_RANGE_DAYS) * DAY_MS;
    return new Date(timeOfDay ? day + random.nextInt((int) DAY_MS) : day);
  }

  private String author() {
    return "author-" + random.nextInt(50);
  }

  private String body(final Database db, final boolean big) {
    if (big)
      return "b".repeat(5 * pageSize(db));
    return "x".repeat(random.nextInt(2_000));
  }

  private int pageSize(final Database db) {
    if (pageSize == 0)
      pageSize = bucket(db).getPageSize();
    return pageSize;
  }

  private static LocalBucket bucket(final Database db) {
    return (LocalBucket) db.getSchema().getType(TYPE).getBuckets(false).getFirst();
  }
}
