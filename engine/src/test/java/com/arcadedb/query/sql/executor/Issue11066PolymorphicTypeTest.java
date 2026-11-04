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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.Document;
import com.arcadedb.database.DocumentCallback;
import com.arcadedb.database.RID;
import com.arcadedb.database.async.DatabaseAsyncExecutor;
import com.arcadedb.engine.ErrorRecordCallback;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.CommandSQLParsingException;
import com.arcadedb.partitioning.PartitioningTestFixture;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.LocalDocumentType;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for #11066: a polymorphic {@code scanType} typed every subtype record as the scanned type,
 * {@code REBUILD TYPE ... POLYMORPHIC WITH repartition = true} re-created every subtype record as a supertype
 * record, and the move phase of a repartition rebuild assumed {@code lookupByRID} returns {@code null} for a
 * deleted record while it throws.
 */
class Issue11066PolymorphicTypeTest extends TestHelper {

  private void createHierarchy() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Base").withTotalBuckets(4).create();
      database.getSchema().buildDocumentType().withName("Sub").withSuperType("Base").withTotalBuckets(4).create();
      database.getSchema().buildDocumentType().withName("SubSub").withSuperType("Sub").withTotalBuckets(2).create();
      database.command("sql", "CREATE PROPERTY Sub.subOnly STRING");
      database.command("sql", "CREATE INDEX ON Sub(subOnly) UNIQUE");
      database.command("sql", "CREATE PROPERTY SubSub.deepOnly STRING");
      database.command("sql", "CREATE INDEX ON SubSub(deepOnly) UNIQUE");
      for (int i = 0; i < 4; i++) {
        database.newDocument("Base").set("n", "base" + i).save();
        database.newDocument("Sub").set("n", "sub" + i).set("subOnly", "s" + i).save();
        database.newDocument("SubSub").set("n", "deep" + i).set("deepOnly", "d" + i).save();
      }
    });
  }

  private long count(final String type) {
    return database.countType(type, false);
  }

  @Test
  void polymorphicScanTypeDeliversTheTypeThatOwnsTheBucket() {
    createHierarchy();

    final Map<String, String> seen = new ConcurrentHashMap<>();
    database.transaction(() -> database.scanType("Base", true, record -> {
      seen.put(record.getString("n"), record.getTypeName());
      return true;
    }));

    assertThat(seen).hasSize(12);
    for (final Map.Entry<String, String> e : seen.entrySet()) {
      final String expected = e.getKey().startsWith("base") ? "Base" : e.getKey().startsWith("sub") ? "Sub" : "SubSub";
      assertThat(e.getValue()).as("type of record " + e.getKey()).isEqualTo(expected);
    }
  }

  /**
   * Every walk of a type agrees with the bucket owner on the type of each record it delivers. The walks are enumerated from
   * the source of truth (issue #11206): every method of {@link Database} and {@link DatabaseAsyncExecutor} taking a type
   * name and a polymorphic flag is either driven here or listed in {@link #NOT_A_WALK} with the reason it delivers no
   * record, so a walk added beside these fails this test until it is wired in. A hand list of the walks passes both before
   * a mis-typing and after a new walk arrives. SQL walks a type too, and is driven explicitly (the hierarchy is of document
   * types, which openCypher does not match).
   */
  @Test
  void everyTypeWalkAgreesWithTheBucketOwner() throws Exception {
    createHierarchy();

    final List<Method> walks = typeWalks();
    assertThat(walks).as("the enumeration no longer finds the walks it is known to cover")
        .extracting(Method::getName).contains("scanType", "iterateType");
    assertThat(walks.stream().filter(m -> m.getDeclaringClass() == DatabaseAsyncExecutor.class)).as("the asynchronous walks")
        .isNotEmpty();

    for (final Method walk : walks)
      assertDeliveredAsTheirBucketOwner(walk.getDeclaringClass().getSimpleName() + "." + walk.getName() + Arrays.toString(
          walk.getParameterTypes()), drive(walk));

    final List<Document> viaSql = new ArrayList<>();
    database.query("sql", "SELECT FROM Base").stream().forEach(r -> viaSql.add(r.toElement()));
    assertDeliveredAsTheirBucketOwner("SQL SELECT", viaSql);
  }

  /** The methods with a type name and a polymorphic flag that deliver no record, each with why. */
  private static final Map<String, String> NOT_A_WALK = Map.of(//
      "countType", "answers a number, delivers no record");

  /** Every method of the two APIs that walks a type: {@code (String typeName, boolean polymorphic, ...)}. */
  private static List<Method> typeWalks() {
    final Map<String, Method> walks = new TreeMap<>();
    for (final Class<?> api : List.of(Database.class, DatabaseAsyncExecutor.class))
      for (final Method method : api.getMethods()) {
        final Class<?>[] parameters = method.getParameterTypes();
        if (parameters.length < 2 || parameters[0] != String.class || parameters[1] != boolean.class)
          continue;
        if (NOT_A_WALK.containsKey(method.getName()))
          continue;
        walks.putIfAbsent(api.getSimpleName() + "." + method.getName() + Arrays.toString(parameters), method);
      }
    return new ArrayList<>(walks.values());
  }

  /**
   * Drives one walk over {@code Base} polymorphically and returns what it delivered. A parameter type it does not know is a
   * walk this test cannot drive yet: wire it in here, or list the method in {@link #NOT_A_WALK}.
   */
  private List<Document> drive(final Method walk) throws Exception {
    final boolean async = walk.getDeclaringClass() == DatabaseAsyncExecutor.class;
    final List<Document> delivered = Collections.synchronizedList(new ArrayList<>());
    final List<Throwable> errors = Collections.synchronizedList(new ArrayList<>());

    final Class<?>[] parameters = walk.getParameterTypes();
    final Object[] arguments = new Object[parameters.length];
    arguments[0] = "Base";
    arguments[1] = true;
    for (int i = 2; i < parameters.length; i++) {
      if (parameters[i] == DocumentCallback.class)
        arguments[i] = (DocumentCallback) record -> delivered.add(record);
      else if (parameters[i] == ErrorRecordCallback.class)
        arguments[i] = (ErrorRecordCallback) (rid, exception) -> errors.add(exception);
      else
        throw new AssertionError("walk " + walk + " takes a " + parameters[i].getName() + " this test cannot drive: wire it into "
            + "drive(), or list the method in NOT_A_WALK with why it delivers no record");
    }

    if (async) {
      final Object result = walk.invoke(database.async(), arguments);
      database.async().waitCompletion();
      assertThat(result).as(walk + " returns something this test does not read").isNull();
    } else
      database.transaction(() -> {
        try {
          final Object result = walk.invoke(database, arguments);
          if (result instanceof Iterator<?> iterator)
            iterator.forEachRemaining(r -> delivered.add((Document) r));
          else
            assertThat(result).as(walk + " returns something this test does not read").isNull();
        } catch (final ReflectiveOperationException e) {
          throw new AssertionError(e);
        }
      });

    assertThat(errors).as(walk.toString()).isEmpty();
    return delivered;
  }

  private void assertDeliveredAsTheirBucketOwner(final String walk, final List<Document> delivered) {
    assertThat(delivered).as(walk).hasSize(12);
    for (final Document d : delivered)
      assertThat(d.getTypeName()).as(walk + " delivered " + d.getIdentity())
          .isEqualTo(database.getSchema().getTypeNameByBucketId(d.getIdentity().getBucketId()));
  }

  @Test
  void truncatePolymorphicConsultsTheSubtypeDeleteListenerAndHonoursItsVeto() {
    database.transaction(() -> {
      database.getSchema().buildDocumentType().withName("Base").create();
      database.getSchema().buildDocumentType().withName("Sub").withSuperType("Base").create();
      database.newDocument("Base").set("n", "b").save();
      database.newDocument("Sub").set("n", "s").save();
    });

    final AtomicInteger subCalls = new AtomicInteger();
    final AtomicInteger baseCalls = new AtomicInteger();
    final DocumentType sub = database.getSchema().getType("Sub");
    final DocumentType base = database.getSchema().getType("Base");
    sub.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      subCalls.incrementAndGet();
      return false;
    });
    base.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      baseCalls.incrementAndGet();
      return true;
    });

    database.command("sql", "TRUNCATE TYPE Base POLYMORPHIC").close();

    assertThat(subCalls.get()).as("the Sub listener must be consulted for the Sub record").isEqualTo(1);
    assertThat(baseCalls.get()).as("the Base listener must see the Base record only").isEqualTo(1);
    assertThat(count("Sub")).as("the record the Sub listener vetoed must survive").isEqualTo(1L);
    assertThat(count("Base")).isEqualTo(0L);
  }

  @Test
  void repartitionWithPolymorphicIsRefusedAndTouchesNothing() {
    createHierarchy();
    final long base = count("Base");
    final long sub = count("Sub");
    final long subSub = count("SubSub");

    assertThatThrownBy(() -> database.command("sql", "REBUILD TYPE Base POLYMORPHIC WITH repartition = true"))
        .isInstanceOf(CommandSQLParsingException.class)
        .hasMessageContaining("POLYMORPHIC");

    assertThat(count("Base")).isEqualTo(base);
    assertThat(count("Sub")).isEqualTo(sub);
    assertThat(count("SubSub")).isEqualTo(subSub);
    for (int i = 0; i < 4; i++) {
      assertThat(database.lookupByKey("Sub", "subOnly", "s" + i).hasNext()).isTrue();
      assertThat(database.lookupByKey("SubSub", "deepOnly", "d" + i).hasNext()).isTrue();
    }
  }

  @Test
  void everyAdmittedRebuildModeKeepsEveryTypeOfTheHierarchyIntact() {
    createHierarchy();
    final String[] types = { "Base", "Sub", "SubSub" };
    final long[] before = new long[types.length];
    for (int i = 0; i < types.length; i++)
      before[i] = count(types[i]);

    for (final boolean polymorphic : new boolean[] { false, true })
      for (final boolean repartition : new boolean[] { false, true }) {
        final String sql = "REBUILD TYPE Base" + (polymorphic ? " POLYMORPHIC" : "") + " WITH repartition = " + repartition;
        try {
          database.command("sql", sql).close();
        } catch (final CommandSQLParsingException e) {
          assertThat(polymorphic && repartition).as("only POLYMORPHIC + repartition may be refused: " + sql).isTrue();
        }
        for (int i = 0; i < types.length; i++)
          assertThat(count(types[i])).as(types[i] + " after: " + sql).isEqualTo(before[i]);
        for (int i = 0; i < 4; i++) {
          assertThat(database.lookupByKey("Sub", "subOnly", "s" + i).hasNext()).as(sql).isTrue();
          assertThat(database.lookupByKey("SubSub", "deepOnly", "d" + i).hasNext()).as(sql).isTrue();
        }
      }
  }

  @Test
  void repartitionSkipsARecordDeletedBetweenTheScanAndTheMove() {
    PartitioningTestFixture.createPartitionedDocType(database, "PartDoc", 4, false);
    database.transaction(() -> {
      for (int i = 0; i < 40; i++)
        database.command("sql", "INSERT INTO PartDoc SET tenant_id = 'tenant" + i + "'");
    });
    final LocalDocumentType type = (LocalDocumentType) database.getSchema().getType("PartDoc");
    type.addBucket((LocalBucket) database.getSchema().createBucket("PartDoc_extra"));
    assertThat(type.isNeedsRepartition()).isTrue();

    // The first record the move phase deletes also deletes every OTHER record of the type, which is what a
    // concurrent delete between the scan's RID capture and the move loop amounts to.
    final AtomicBoolean fired = new AtomicBoolean();
    type.getEvents().registerListener((BeforeRecordDeleteListener) record -> {
      if (fired.compareAndSet(false, true)) {
        final List<RID> others = new ArrayList<>();
        database.scanType("PartDoc", false, d -> {
          if (!d.getIdentity().equals(record.getIdentity()))
            others.add(d.getIdentity());
          return true;
        });
        for (final RID rid : others)
          database.deleteRecord(database.lookupByRID(rid, false));
      }
      return true;
    });

    final Result row;
    try (final ResultSet rs = database.command("sql", "REBUILD TYPE PartDoc WITH repartition = true")) {
      row = rs.next();
    }

    assertThat(row.<Long>getProperty("recordsSkipped"))
        .as("records deleted before the move reached them must be skipped and reported")
        .isGreaterThan(0L);
    assertThat(row.<Long>getProperty("recordsRebuilt") + row.<Long>getProperty("recordsSkipped"))
        .as("a skipped record is not a rebuilt one: every visited record is exactly one of the two")
        .isEqualTo(40L);
    assertThat(count("PartDoc"))
        .as("only the record the move itself re-created may remain, no ghost of a deleted one")
        .isEqualTo(1L);
    assertThat(type.isNeedsRepartition()).isFalse();
  }
}
