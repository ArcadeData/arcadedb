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
package com.arcadedb.integration.exporter;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.Identifiable;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.integration.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;
import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The restore of a JSONL export whose single record links to thousands of records the export writes after it (the exporter writes
 * every vertex before every document): the links are resolved once the whole file is read, and the work of that step grows with the
 * number of links, not with its square.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class JsonlImportManyForwardLinksTest {
  private static final String SOURCE_PATH = "target/databases/jsonl-many-links-source";
  private static final String TARGET_PATH = "target/databases/jsonl-many-links-target";
  private static final String FILE        = "jsonl-many-links.jsonl.tgz";

  private static final int  LINKS               = 20_000;
  /** Every n-th target is deleted before the export: its link names a record that is not in the file. */
  private static final int  DELETE_NTH          = 7;
  /**
   * What the import of the links may allocate in all: about 6 KB for each of them, so some 120 MB. A copy of the whole container
   * for every link is LINKS * LINKS * 4 bytes at the least, which is 1.6 GB here.
   */
  private static final long MAX_ALLOCATED_BYTES = 400L * 1024 * 1024;

  @BeforeEach
  @AfterEach
  void clean() {
    TestHelper.checkActiveDatabases();
    FileUtils.deleteRecursively(new File(SOURCE_PATH));
    FileUtils.deleteRecursively(new File(TARGET_PATH));
    new File("exports/" + FILE).delete();
  }

  @Test
  void aListOfLinksToRecordsExportedLaterIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p LIST OF LINK");
      fill(source, (holder, rids) -> holder.set("p", rids));
      deleteEveryNthTarget(source);
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      final long dangling = importInto(target);

      assertThat(dangling).isEqualTo(deletedCount());
      assertThat(idsOf(holderOf(target).getList("p"))).containsExactlyElementsOf(liveIds());
    }
  }

  @Test
  void aMapOfLinksToRecordsExportedLaterIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      source.command("sql", "CREATE PROPERTY Holder.p MAP OF LINK");
      fill(source, (holder, rids) -> {
        final Map<String, RID> map = new LinkedHashMap<>();
        for (int i = 0; i < rids.size(); i++)
          map.put("k" + i, rids.get(i));
        holder.set("p", map);
      });
      deleteEveryNthTarget(source);
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      final long dangling = importInto(target);

      assertThat(dangling).isEqualTo(deletedCount());
      final Map<String, Object> restored = holderOf(target).getMap("p");
      assertThat(restored).hasSize(LINKS - (int) deletedCount());
      for (int i = 0; i < LINKS; i++) {
        final Object link = restored.get("k" + i);
        if (isDeleted(i))
          assertThat(link).as("k" + i).isNull();
        else
          assertThat(idOf(link)).as("k" + i).isEqualTo(i);
      }
    }
  }

  @Test
  void anUndeclaredListOfLinksToRecordsExportedLaterIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      fill(source, (holder, rids) -> holder.set("free", new ArrayList<Object>(rids)));
      deleteEveryNthTarget(source);
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      final long dangling = importInto(target);

      // AN UNDECLARED LIST KEEPS ITS POSITIONS: A LINK WHOSE TARGET IS NOT IN THE FILE IS A NULL
      assertThat(dangling).isEqualTo(deletedCount());
      final List<Object> restored = holderOf(target).getList("free");
      assertThat(restored).hasSize(LINKS);
      for (int i = 0; i < LINKS; i++) {
        if (isDeleted(i))
          assertThat(restored.get(i)).as("position " + i).isNull();
        else
          assertThat(idOf(restored.get(i))).as("position " + i).isEqualTo(i);
      }
    }
  }

  @Test
  void anEmbeddedListOfLinksToRecordsExportedLaterIsRestored() {
    try (final Database source = new DatabaseFactory(SOURCE_PATH).create()) {
      source.command("sql", "CREATE DOCUMENT TYPE Target");
      source.command("sql", "CREATE PROPERTY Target.id LONG");
      source.command("sql", "CREATE VERTEX TYPE Holder");
      fill(source, (holder, rids) -> holder.newEmbeddedDocument("Target", "emb").set("refs", new ArrayList<Object>(rids)));
      deleteEveryNthTarget(source);
      source.command("sql", "EXPORT DATABASE file://" + FILE + " WITH overwrite = true");
    }

    try (final Database target = new DatabaseFactory(TARGET_PATH).create()) {
      final long dangling = importInto(target);

      assertThat(dangling).isEqualTo(deletedCount());
      final List<Object> restored = ((Document) holderOf(target).get("emb")).getList("refs");
      assertThat(restored).hasSize(LINKS);
      for (int i = 0; i < LINKS; i++) {
        if (isDeleted(i))
          assertThat(restored.get(i)).as("position " + i).isNull();
        else
          assertThat(idOf(restored.get(i))).as("position " + i).isEqualTo(i);
      }
    }
  }

  /** The Holder vertex first, so that it is exported before the documents it links to, then the targets and the links. */
  private static void fill(final Database source, final LinkSetter setter) {
    source.transaction(() -> {
      final MutableVertex holder = source.newVertex("Holder").save();
      final List<RID> rids = new ArrayList<>(LINKS);
      for (int i = 0; i < LINKS; i++) {
        final MutableDocument target = source.newDocument("Target").set("id", (long) i);
        target.save();
        rids.add(target.getIdentity());
      }
      setter.set(holder, rids);
      holder.save();
    });
  }

  private static void deleteEveryNthTarget(final Database source) {
    source.transaction(() -> {
      final List<RID> doomed = new ArrayList<>();
      final Iterator<Record> targets = source.iterateType("Target", false);
      while (targets.hasNext()) {
        final Document target = targets.next().asDocument();
        if (isDeleted(target.getLong("id").intValue()))
          doomed.add(target.getIdentity());
      }
      for (final RID rid : doomed)
        source.deleteRecord(rid.asDocument());
    });
  }

  private static boolean isDeleted(final int index) {
    return index % DELETE_NTH == 0;
  }

  private static long deletedCount() {
    return (LINKS + DELETE_NTH - 1) / DELETE_NTH;
  }

  private static List<Long> liveIds() {
    final List<Long> ids = new ArrayList<>();
    for (int i = 0; i < LINKS; i++)
      if (!isDeleted(i))
        ids.add((long) i);
    return ids;
  }

  /** Imports the file and returns the number of links that named a record the file does not hold. */
  private static long importInto(final Database target) {
    final ThreadMXBean threads = (ThreadMXBean) ManagementFactory.getThreadMXBean();
    // A JVM THAT CANNOT COUNT THE BYTES OF A THREAD ANSWERS -1 TO BOTH SAMPLES, WHICH SUBTRACT TO A PASS: THEN ONLY THE BYTES ARE NOT CHECKED
    final boolean counted = threads.isThreadAllocatedMemorySupported();
    if (counted)
      threads.setThreadAllocatedMemoryEnabled(true);
    final long threadId = Thread.currentThread().threadId();
    final long before = counted ? threads.getThreadAllocatedBytes(threadId) : 0L;
    final long dangling;
    try (final ResultSet rs = target.command("sql", "IMPORT DATABASE file://" + new File("exports/" + FILE).getAbsolutePath())) {
      dangling = rs.next().<Long>getProperty("danglingLinks");
    }
    if (counted) {
      final long after = threads.getThreadAllocatedBytes(threadId);
      assertThat(before).as("allocated bytes of the thread before the import").isNotNegative();
      assertThat(after).as("allocated bytes of the thread after the import").isNotNegative();
      // COUNTED, NOT TIMED: A COPY OF THE WHOLE CONTAINER FOR EACH OF ITS LINKS IS BYTES NO PAUSE OF THE JVM CAN HIDE OR ADD
      assertThat(after - before).as("bytes allocated by the import of " + LINKS + " links").isLessThan(MAX_ALLOCATED_BYTES);
    }
    return dangling;
  }

  private static Document holderOf(final Database db) {
    try (final ResultSet rs = db.query("sql", "SELECT FROM Holder")) {
      return rs.next().getElement().get();
    }
  }

  private static List<Long> idsOf(final List<Object> links) {
    final List<Long> ids = new ArrayList<>(links.size());
    for (final Object link : links)
      ids.add(idOf(link));
    return ids;
  }

  private static long idOf(final Object link) {
    return ((Identifiable) link).asDocument().getLong("id");
  }

  private interface LinkSetter {
    void set(MutableVertex holder, List<RID> rids);
  }
}
