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
package com.arcadedb.bench;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.index.Index;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.log.LogManager;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.util.concurrent.TimeUnit;

/**
 * JMH microbenchmarks of the read paths that every query walks: the property read of a loaded record, the index lookup, the read
 * of a record that spans pages, and the hash of a RID.
 * <p>
 * A load test measures the whole system, where the run-to-run spread is tens of percent, so a change of one percent in one of
 * these paths cannot be read from it. JMH can: it runs the same operation in a loop, warms the JIT up and forks its own JVM, so
 * the difference between two builds is the difference between the two numbers and not the noise of the host.
 * <p>
 * Surefire does not run it: the class has no JUnit test, and a {@code main} is not a test. Start the JMH runner in its own JVM
 * with the compiled test classes and the test dependencies on the classpath, from the repository root. {@code proc=full} makes
 * javac run the JMH annotation processor, which a JDK from 23 on skips by default, and without which JMH finds no benchmark:
 * <pre>
 * mvn -pl engine -am test-compile -Dmaven.compiler.proc=full
 * mvn -q -pl engine dependency:build-classpath -Dmdep.includeScope=test -Dmdep.outputFile=target/test.cp
 * java --add-modules jdk.incubator.vector -cp engine/target/test-classes:engine/target/classes:$(cat engine/target/test.cp) \
 *   com.arcadedb.bench.ReadPathJMHBenchmark
 * </pre>
 * or run {@code main} from an IDE. The numbers are nanoseconds per operation.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = { "-Xms1G", "-Xmx1G", "--enable-native-access=ALL-UNNAMED", "--add-modules", "jdk.incubator.vector" })
public class ReadPathJMHBenchmark {
  private static final String DB_PATH = "target/databases/read-path-benchmark";

  @State(Scope.Benchmark)
  public static class Data {
    Database  database;
    Document  record;
    Document  bigRecord;
    RID       rid;
    RID       bigRid;
    Index     index;
  }

  @Setup(Level.Trial)
  public void setup(final Data state) {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();

    state.database = factory.create();
    state.database.getSchema().createDocumentType("Doc");
    state.database.getSchema().getType("Doc").createProperty("name", Type.STRING);
    state.database.getSchema().getType("Doc").createProperty("n", Type.INTEGER);
    state.database.getSchema().getType("Doc").createProperty("payload", Type.STRING);
    state.database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "Doc", "name");

    // the records are written in batches: one transaction with all of them would hold 400 MB of changes
    final String payload = "x".repeat(70_000);
    state.database.begin();
    for (int i = 0; i < 500; i++) {
      state.database.newDocument("Doc").set("name", "name-" + i).set("n", i).set("payload", payload).save();
      if (i % 25 == 24) {
        state.database.commit();
        state.database.begin();
      }
    }
    state.database.commit();
    // the records are read once, so the benchmarks read what a query reads: a loaded record
    final RID first = state.database.query("sql", "SELECT FROM Doc LIMIT 1").next().toElement().getIdentity();
    state.record = state.database.lookupByRID(first, true).asDocument();
    final RID big = state.database.query("sql", "SELECT FROM Doc WHERE n = 0").next().toElement().getIdentity();
    state.bigRecord = state.database.lookupByRID(big, true).asDocument();
    state.rid = state.record.getIdentity();
    state.bigRid = state.bigRecord.getIdentity();
    state.index = state.database.getSchema().getIndexByName("Doc[name]");
  }

  @TearDown(Level.Trial)
  public void tearDown(final Data state) {
    if (state.database != null)
      state.database.drop();
    new File(DB_PATH).delete();
  }

  /** The property read of a loaded record: the dictionary lookup and the record's own bytes. */
  @Benchmark
  public void propertyRead(final Data state, final Blackhole blackhole) {
    blackhole.consume(state.record.get("name"));
  }

  /** The names of the properties of a loaded record: one dictionary lookup per name. */
  @Benchmark
  public void propertyNames(final Data state, final Blackhole blackhole) {
    blackhole.consume(state.record.getPropertyNames());
  }

  /** The lookup of a key in a UNIQUE index, and the load of the record it points at. */
  @Benchmark
  public void indexLookup(final Data state, final Blackhole blackhole) {
    final IndexCursor cursor = state.index.get(new Object[] { "name-42" });
    blackhole.consume(cursor.next().getRecord());
  }

  /** The read of a record that spans several pages: the chain of its chunks is assembled. */
  @Benchmark
  public void multiPageRecordRead(final Data state, final Blackhole blackhole) {
    blackhole.consume(state.database.lookupByRID(state.bigRid, true).asDocument().get("n"));
  }

  /** A log call at a level that is disabled: the gate, and what the call used to build before it asked. */
  @Benchmark
  public void disabledLogCall(final Data state, final Blackhole blackhole) {
    LogManager.instance().log(this, java.util.logging.Level.FINE, "a message nobody reads %s", "an argument");
  }

  /** The hash of a RID: it is on the path of every map that holds a record. */
  @Benchmark
  public void ridHash(final Data state, final Blackhole blackhole) {
    blackhole.consume(state.rid.hashCode());
  }

  public static void main(final String[] args) throws RunnerException {
    final Options options = new OptionsBuilder().include(ReadPathJMHBenchmark.class.getSimpleName()).build();
    new Runner(options).run();
  }
}
