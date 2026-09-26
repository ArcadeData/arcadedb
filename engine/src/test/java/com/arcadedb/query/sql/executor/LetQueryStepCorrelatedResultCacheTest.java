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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.Identifiable;
import com.arcadedb.function.sql.DefaultSQLFunctionFactory;
import com.arcadedb.function.sql.SQLFunctionAbstract;
import com.arcadedb.query.sql.method.DefaultSQLMethodFactory;
import com.arcadedb.query.sql.parser.Statement;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8400: a per-record {@code LET $x = (subquery)} re-executed its correlated subquery for every row even when
 * many rows fed it the same outer values. {@link LetQueryStep} now remembers the result per distinct binding of the
 * outer variables the subquery actually read ({@link CorrelatedSubQueryCache}).
 * <p>
 * The fixture is the shape from the issue: many investigations ({@code Leaf}) under a few offices ({@code Mid}), each
 * office under its own regional root. Every test that expects hits also checks every row's value, because the
 * failure a result cache can introduce is a row answered with another binding's result.
 */
class LetQueryStepCorrelatedResultCacheTest extends TestHelper {

  private static final int OFFICES           = 3;
  private static final int LEAVES_PER_OFFICE = 5;

  private static final String ISSUE_QUERY =
      "select name, $uf.name as parentName, $root[0].name as rootName " +
          "from CacheNode " +
          "let " +
          "  $uf = out('CacheNode_parent')[0], " +
          "  $root = (select from (traverse out('CacheNode_parent') from (select $parent.uf)) where isRoot = true) " +
          "where name like 'Leaf%' " +
          "order by name";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType("CacheNode");
    database.getSchema().createEdgeType("CacheNode_parent");

    database.transaction(() -> {
      for (int o = 1; o <= OFFICES; o++) {
        database.command("sql", "create vertex CacheNode set name = 'Root" + o + "', isRoot = true, office = " + o).close();
        database.command("sql", "create vertex CacheNode set name = 'Mid" + o + "', office = " + o).close();
        database.command("sql",
            "create edge CacheNode_parent from (select from CacheNode where name = 'Mid" + o + "') to (select from CacheNode where name = 'Root"
                + o + "')").close();
        for (int l = 1; l <= LEAVES_PER_OFFICE; l++) {
          final String leaf = leafName(o, l);
          database.command("sql", "create vertex CacheNode set name = '" + leaf + "', office = " + o + ", flag = " + (l % 2 == 0)).close();
          database.command("sql",
              "create edge CacheNode_parent from (select from CacheNode where name = '" + leaf + "') to (select from CacheNode where name = 'Mid"
                  + o + "')").close();
        }
      }
    });
  }

  private static String leafName(final int office, final int leaf) {
    return "Leaf" + office + "_" + leaf;
  }

  @Test
  void subqueryRunsOncePerDistinctBindingAndEveryRowKeepsItsOwnResult() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", ISSUE_QUERY);
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final String name = row.getProperty("name");
        final String office = name.substring("Leaf".length(), name.indexOf('_'));
        assertThat(row.<String>getProperty("parentName")).isEqualTo("Mid" + office);
        assertThat(row.<String>getProperty("rootName")).isEqualTo("Root" + office);
        ++rows;
      }
      assertThat(rows).isEqualTo(OFFICES * LEAVES_PER_OFFICE);

      final CorrelatedSubQueryCache cache = rootLetStep(rs).getResultCache();
      assertThat(cache).as("result cache in use").isNotNull();
      assertThat(cache.isDisabled()).isFalse();
      assertThat(cache.getMisses()).as("one execution per distinct $uf").isEqualTo(OFFICES);
      assertThat(cache.getHits()).isEqualTo(OFFICES * LEAVES_PER_OFFICE - OFFICES);
      rs.close();
    });
  }

  @Test
  void keyIsTheOuterValueTheSubqueryRead() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", ISSUE_QUERY);
      while (rs.hasNext())
        rs.next();

      final Set<String> names = new HashSet<>();
      for (final CorrelatedSubQueryCache.Dependency d : rootLetStep(rs).getResultCache().getDependencies())
        names.add(d.name());
      assertThat(names).contains("uf");
      // THE OUTER ROW ITSELF IS NOT PART OF THE KEY, OR NO TWO ROWS WOULD EVER SHARE AN ENTRY
      assertThat(names).doesNotContain("current", "$current");
      rs.close();
    });
  }

  /**
   * A subquery that reaches its outer row through {@code $parent.$current} directly: the key is the whole row, so no
   * two rows share an entry, and every row must still get its own answer.
   */
  @Test
  void parentCurrentReadThroughTheParentViewKeysOnTheRow() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select name, $same[0].name as sameName from CacheNode " +
              "let $same = (select from CacheNode where name = $parent.$current.name) " +
              "where name like 'Leaf%' order by name");
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("sameName")).isEqualTo(row.<String>getProperty("name"));
        ++rows;
      }
      assertThat(rows).isEqualTo(OFFICES * LEAVES_PER_OFFICE);

      final CorrelatedSubQueryCache cache = findLetStep(rs, "same").getResultCache();
      assertThat(cache).isNotNull();
      assertThat(cache.getHits()).isZero();
      rs.close();
    });
  }

  /**
   * The same shape keyed through a per-record LET expression instead: the subquery reads {@code $parent.office}, a
   * value shared by every leaf of one office, so it runs once per office.
   */
  @Test
  void parentVariableReadThroughTheParentViewIsShared() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select name, office, $peers.size() as peers from CacheNode " +
              "let $office = office, $peers = (select name, office from CacheNode where office = $parent.office and name like 'Leaf%') " +
              "where name like 'Leaf%' order by name");
      while (rs.hasNext())
        assertThat(rs.next().<Integer>getProperty("peers")).isEqualTo(LEAVES_PER_OFFICE);

      final CorrelatedSubQueryCache cache = findLetStep(rs, "peers").getResultCache();
      assertThat(cache).isNotNull();
      assertThat(cache.isDisabled()).isFalse();
      assertThat(cache.getMisses()).isEqualTo(OFFICES);
      assertThat(cache.getHits()).isEqualTo(OFFICES * LEAVES_PER_OFFICE - OFFICES);
      rs.close();
    });
  }

  /**
   * The reads can differ between runs: {@code AND} stops at its first false operand, so {@code $parent.office} is
   * read only for a row whose flag is true. The first row (flag false) records only {@code flag}; if the key stayed
   * at that, every later true row would be answered with the first true row's entry - office 1's root - whatever its
   * own office. The cache must widen its key to every read seen and drop what it cached under the narrower one.
   */
  @Test
  void aReadFirstSeenOnALaterRowWidensTheKeyInsteadOfServingAStaleEntry() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select name, office, flag, $hit[0].name as rootName, $hit.size() as hitCount from CacheNode " +
              "let $flag = flag, $office = office, " +
              "    $hit = (select from CacheNode where isRoot = true and $parent.flag = true and office = $parent.office) " +
              "where name like 'Leaf%'");
      int rows = 0;
      while (rs.hasNext()) {
        final Result row = rs.next();
        final boolean flag = row.getProperty("flag");
        final int office = row.getProperty("office");
        assertThat(row.<Integer>getProperty("hitCount")).as(row.toString()).isEqualTo(flag ? 1 : 0);
        assertThat(row.<String>getProperty("rootName")).as(row.toString()).isEqualTo(flag ? "Root" + office : null);
        ++rows;
      }
      assertThat(rows).isEqualTo(OFFICES * LEAVES_PER_OFFICE);

      final CorrelatedSubQueryCache cache = findLetStep(rs, "hit").getResultCache();
      assertThat(cache).isNotNull();
      assertThat(cache.getHits()).as("the cache is exercised, not bypassed").isPositive();
      rs.close();
    });
  }

  /**
   * A subquery that scans a multi-bucket type, run outside a transaction, fetches in parallel: each worker gets its
   * own copy of the subquery's context ({@link BasicCommandContext#copy()}). The copies must stay tracking contexts
   * that share one tracker, so the fixture drives that path and checks every row's rows still carry its own binding.
   */
  @Test
  void parallelScanOfTheSubqueryKeepsTrackingThroughWorkerContextCopies() {
    database.command("sql", "create vertex type ParNode buckets 4").close();
    database.transaction(() -> {
      for (int i = 0; i < 16; i++)
        database.command("sql", "create vertex ParNode set seq = " + i).close();
    });

    final ResultSet explain = database.query("sql", "explain select seq from ParNode");
    assertThat(explain.getExecutionPlan().get().prettyPrint(0, 2)).as("fixture scans in parallel").contains("(parallel)");
    explain.close();

    final ResultSet rs = database.query("sql",
        "select name, office, $par as par from CacheNode " +
            "let $office = office, $par = (select seq, $parent.office as po from ParNode) " +
            "where name like 'Leaf%'");
    int rows = 0;
    while (rs.hasNext()) {
      final Result row = rs.next();
      final List<Result> par = row.getProperty("par");
      assertThat(par).hasSize(16);
      for (final Result r : par)
        assertThat(r.<Integer>getProperty("po")).isEqualTo(row.<Integer>getProperty("office"));
      ++rows;
    }
    assertThat(rows).isEqualTo(OFFICES * LEAVES_PER_OFFICE);

    final CorrelatedSubQueryCache cache = findLetStep(rs, "par").getResultCache();
    assertThat(cache.isDisabled()).isFalse();
    assertThat(cache.getMisses()).isEqualTo(OFFICES);
    assertThat(cache.getHits()).isEqualTo(OFFICES * LEAVES_PER_OFFICE - OFFICES);
    rs.close();
  }

  /**
   * {@code $root} walks past the outer context, which the tracker cannot follow: the first run disables the cache,
   * every row still gets exactly what it gets with the cache off, and no later row goes through a lookup again.
   */
  @Test
  void anUntrackableReadDisablesTheCacheForTheRestOfTheExecution() {
    final String query = "select name, $peers.size() as peers from CacheNode " +
        "let $office = office, $peers = (select from CacheNode where office = $root.office or name = $parent.name) " +
        "where name like 'Leaf%' order by name";

    final List<Object> withoutCache = new ArrayList<>();
    database.getConfiguration().setValue(GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE, 0);
    try {
      database.transaction(() -> {
        final ResultSet rs = database.query("sql", query);
        while (rs.hasNext())
          withoutCache.add(rs.next().getProperty("peers"));
        rs.close();
      });
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE,
          GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE.getDefValue());
    }

    database.transaction(() -> {
      final ResultSet rs = database.query("sql", query);
      final List<Object> withCache = new ArrayList<>();
      while (rs.hasNext())
        withCache.add(rs.next().getProperty("peers"));
      assertThat(withCache).hasSize(OFFICES * LEAVES_PER_OFFICE).isEqualTo(withoutCache);

      final CorrelatedSubQueryCache cache = findLetStep(rs, "peers").getResultCache();
      assertThat(cache.isDisabled()).isTrue();
      assertThat(cache.getHits()).isZero();
      assertThat(cache.getMisses()).as("only the first row looked the cache up").isEqualTo(1);
      rs.close();
    });
  }

  @Test
  void nonDeterministicSubqueryIsNeverCached() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select name, $r[0].id as id from CacheNode " +
              "let $uf = out('CacheNode_parent')[0], $r = (select uuid() as id, $parent.uf as uf) " +
              "where name like 'Leaf%'");
      final Set<String> ids = new HashSet<>();
      while (rs.hasNext())
        ids.add(rs.next().getProperty("id"));
      assertThat(ids).hasSize(OFFICES * LEAVES_PER_OFFICE);
      assertThat(findLetStep(rs, "r").getResultCache()).isNull();
      rs.close();
    });
  }

  @Test
  void nonDeterministicCallInTheEnclosingStatementDisablesTheCache() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select name, randomInt(10) as r, $root[0].name as rootName from CacheNode " +
              "let $uf = out('CacheNode_parent')[0], " +
              "    $root = (select from (traverse out('CacheNode_parent') from (select $parent.uf)) where isRoot = true) " +
              "where name like 'Leaf%'");
      while (rs.hasNext())
        rs.next();
      assertThat(rootLetStep(rs).getResultCache()).isNull();
      rs.close();
    });
  }

  @Test
  void zeroCacheSizeDisablesTheCache() {
    database.getConfiguration().setValue(GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE, 0);
    try {
      database.transaction(() -> {
        final ResultSet rs = database.query("sql", ISSUE_QUERY);
        int rows = 0;
        while (rs.hasNext()) {
          rs.next();
          ++rows;
        }
        assertThat(rows).isEqualTo(OFFICES * LEAVES_PER_OFFICE);
        assertThat(rootLetStep(rs).getResultCache()).isNull();
        rs.close();
      });
    } finally {
      database.getConfiguration().setValue(GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE,
          GlobalConfiguration.SQL_LET_SUBQUERY_CACHE_SIZE.getDefValue());
    }
  }

  /**
   * The result set is lazy, so a caller can change the database between two {@code next()} calls. A row whose binding
   * was already cached must still see that change.
   */
  @Test
  void aChangeMadeWhileIteratingIsSeenByTheNextRowWithTheSameBinding() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql",
          "select name, $root[0].label as rootLabel from CacheNode " +
              "let $uf = out('CacheNode_parent')[0], " +
              "    $root = (select from (traverse out('CacheNode_parent') from (select $parent.uf)) where isRoot = true) " +
              // NO ORDER BY: SORTING WOULD DRAIN EVERY ROW, AND SO EVERY LET, BEFORE THE FIRST next() RETURNS
              "where name like 'Leaf1_%'");

      final List<Object> labels = new ArrayList<>();
      labels.add(rs.next().getProperty("rootLabel"));
      database.command("sql", "update CacheNode set label = 'changed' where name = 'Root1'").close();
      while (rs.hasNext())
        labels.add(rs.next().getProperty("rootLabel"));

      assertThat(labels).hasSize(LEAVES_PER_OFFICE);
      assertThat(labels.getFirst()).isNull();
      assertThat(labels.subList(1, labels.size())).containsOnly("changed");
      rs.close();
    });
  }

  @Test
  void profileReportsTheCacheCounters() {
    database.transaction(() -> {
      final ResultSet rs = database.query("sql", "PROFILE " + ISSUE_QUERY);
      assertThat(rs.getExecutionPlan().get().prettyPrint(0, 2)).contains("result cache: " + (OFFICES * LEAVES_PER_OFFICE - OFFICES) + " hits");
      rs.close();
    });
  }

  @Test
  void staticCheckAdmitsTheIssueShapeAndRejectsSideEffectsAndNonRepeatableCalls() {
    assertThat(CorrelatedSubQueryCache.isCacheable(parse(ISSUE_QUERY))).isTrue();
    // THE ANSWER IS MEMOIZED ON THE PARSED INSTANCE THE STATEMENT CACHE HANDS OUT, NOT RECOMPUTED PER EXECUTION
    assertThat(parse(ISSUE_QUERY).resultCacheable).isTrue();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select from CacheNode where out('CacheNode_parent').size() > 0 or name.out() is null"))).isTrue();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select count(*) as c, max(office) from CacheNode where office = $parent.office"))).isTrue();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select name.toLowerCase() from CacheNode"))).isTrue();

    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select uuid() from CacheNode"))).isFalse();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select sysdate() from CacheNode"))).isFalse();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select date() from CacheNode"))).isFalse();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select randomInt(5) from CacheNode"))).isFalse();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select eval('1 + 1') from CacheNode"))).isFalse();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select mylib.myfn(name) from CacheNode"))).isFalse();
    // NESTED DEEP INSIDE A FROM-SUBQUERY STILL COUNTS
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select from (select from (select uuid() as u)) where u is not null"))).isFalse();
    assertThat(CorrelatedSubQueryCache.isCacheable(parse("update CacheNode set x = 1"))).isFalse();
  }

  /**
   * Built-in functions reviewed as answering the same for the same arguments within one execution, and without side
   * effects. {@link CorrelatedSubQueryCache} admits every registered built-in not in its deny list, so a newly
   * registered function that reads the clock or a random source would silently be cached: this test fails on any
   * registered name that is in neither list, until it is classified.
   */
  private static final Set<String> REVIEWED_REPEATABLE_FUNCTIONS = Set.of(
      "abs", "astar", "avg", "bellmanford", "bool_and", "bool_or", "both", "bothe", "bothv", "circle",
      "coalesce", "concat", "count", "cypherrid", "date", "decode", "difference", "dijkstra", "distance",
      "duansssp", "duration", "encode", "first", "format", "fulltext.searchfields",
      "fulltext.searchfieldsmore", "fulltext.searchindex", "fulltext.searchindexmore", "geo.area",
      "geo.asgeojson", "geo.astext", "geo.buffer", "geo.contains", "geo.crosses", "geo.disjoint",
      "geo.distance", "geo.dwithin", "geo.envelope", "geo.equals", "geo.geomfromtext", "geo.intersects",
      "geo.linestring", "geo.overlaps", "geo.point", "geo.polygon", "geo.touches", "geo.within", "geo.x",
      "geo.y", "if", "ifempty", "ifnull", "in", "ine", "intersect", "inv", "last", "linestring", "list",
      "map", "math_abs", "math_absexact", "math_acos", "math_addexact", "math_asin", "math_atan",
      "math_atan2", "math_cbrt", "math_ceil", "math_ceildiv", "math_ceildivexact", "math_ceilmod",
      "math_clamp", "math_copysign", "math_cos", "math_cosh", "math_decrementexact", "math_divideexact",
      "math_exp", "math_expm1", "math_floor", "math_floordiv", "math_floordivexact", "math_floormod",
      "math_fma", "math_getexponent", "math_hypot", "math_ieeeremainder", "math_incrementexact", "math_log",
      "math_log10", "math_log1p", "math_max", "math_min", "math_multiplyexact", "math_multiplyfull",
      "math_multiplyhigh", "math_negateexact", "math_nextafter", "math_nextdown", "math_nextup", "math_pow",
      "math_powexact", "math_rint", "math_round", "math_scalb", "math_signum", "math_sin", "math_sinh",
      "math_sqrt", "math_subtractexact", "math_tan", "math_tanh", "math_todegrees", "math_tointexact",
      "math_toradians", "math_ulp", "math_unsignedmultiplyexact", "math_unsignedmultiplyhigh",
      "math_unsignedpowexact", "max", "median", "min", "mode", "out", "oute", "outv", "percentile", "point",
      "polygon", "pow", "rectangle", "search_fields", "search_fields_more", "search_index",
      "search_index_more", "set", "shortestpath", "sqrt", "stddev", "stddevp", "strcmpci", "sum",
      "symmetricdifference", "ts.correlate", "ts.delta", "ts.first", "ts.interpolate", "ts.lag", "ts.last",
      "ts.lead", "ts.movingavg", "ts.percentile", "ts.rank", "ts.rate", "ts.rownumber", "ts.timebucket",
      "unionall", "variance", "variancep", "vector.add", "vector.approxdistance", "vector.avg",
      "vector.boost", "vector.clamp", "vector.clip", "vector.cosinesimilarity", "vector.densetosparse",
      "vector.dequantizebinary", "vector.dequantizeint8", "vector.dimension", "vector.discover",
      "vector.dotproduct", "vector.fuse", "vector.hasinf", "vector.hasnan", "vector.hasnull",
      "vector.hybridscore", "vector.isnormalized", "vector.l1distance", "vector.l1norm", "vector.l2distance",
      "vector.l2norm", "vector.linfnorm", "vector.magnitude", "vector.manhattandistance", "vector.max",
      "vector.min", "vector.mmr", "vector.multiply", "vector.multiscore", "vector.neighbors",
      "vector.normalize", "vector.normalizescores", "vector.quantizebinary", "vector.quantizeint8",
      "vector.recommend", "vector.rerank", "vector.rrfscore", "vector.scale", "vector.scoretransform",
      "vector.sparsecreate", "vector.sparsedot", "vector.sparseneighbors", "vector.sparsetodense",
      "vector.sparsity", "vector.stddev", "vector.subtract", "vector.sum", "vector.tostring",
      "vector.variance", "vectoradd", "vectorapproxdistance", "vectoravg", "vectorboost", "vectorclamp",
      "vectorclip", "vectorcosinesimilarity", "vectordensetosparse", "vectordequantizebinary",
      "vectordequantizeint8", "vectordimension", "vectordiscover", "vectordotproduct", "vectorfuse",
      "vectorhasinf", "vectorhasnan", "vectorhasnull", "vectorhybridscore", "vectorisnormalized",
      "vectorl1distance", "vectorl1norm", "vectorl2distance", "vectorl2norm", "vectorlinfnorm",
      "vectormagnitude", "vectormanhattandistance", "vectormax", "vectormin", "vectormmr", "vectormultiply",
      "vectormultiscore", "vectorneighbors", "vectornormalize", "vectornormalizescores",
      "vectorquantizebinary", "vectorquantizeint8", "vectorrecommend", "vectorrerank", "vectorrrfscore",
      "vectorscale", "vectorscoretransform", "vectorsparsecreate", "vectorsparsedot", "vectorsparseneighbors",
      "vectorsparsetodense", "vectorsparsity", "vectorstddev", "vectorsubtract", "vectorsum",
      "vectortostring", "vectorvariance", "version");

  /** Built-in methods reviewed as not mutating their receiver or arguments; see REVIEWED_REPEATABLE_FUNCTIONS. */
  private static final Set<String> REVIEWED_NON_MUTATING_METHODS = Set.of(
      "append", "asboolean", "asbyte", "ascypherrid", "asdate", "asdatetime", "asdecimal", "asdouble",
      "asfloat", "asinteger", "asjson", "aslist", "aslong", "asmap", "asrecord", "asrid", "asset", "asshort",
      "assparse", "asstring", "asvector", "capitalize", "charat", "convert", "exclude", "field", "first",
      "format", "hash", "ifempty", "ifnull", "include", "indexof", "intersectswith", "iswithin", "javatype",
      "join", "keys", "last", "lastindexof", "left", "length", "normalize", "precision", "prefix", "replace",
      "right", "size", "sort", "split", "substring", "tojson", "tolowercase", "touppercase", "transform",
      "trim", "trimprefix", "trimsuffix", "type", "values");

  @Test
  void everyRegisteredBuiltInIsClassified() {
    final Set<String> functions = new TreeSet<>(DefaultSQLFunctionFactory.getInstance().getBuiltInFunctionNames());
    functions.removeAll(REVIEWED_REPEATABLE_FUNCTIONS);
    functions.removeAll(CorrelatedSubQueryCache.NON_REPEATABLE_FUNCTIONS);
    assertThat(functions).as("classify these functions in CorrelatedSubQueryCache.NON_REPEATABLE_FUNCTIONS or in this test").isEmpty();

    final Set<String> methods = new TreeSet<>(DefaultSQLMethodFactory.getInstance().getBuiltInMethodNames());
    methods.removeAll(REVIEWED_NON_MUTATING_METHODS);
    methods.removeAll(CorrelatedSubQueryCache.MUTATING_METHODS);
    assertThat(methods).as("classify these methods in CorrelatedSubQueryCache.MUTATING_METHODS or in this test").isEmpty();

    assertThat(CorrelatedSubQueryCache.isCacheable(parse("select math_random() from CacheNode"))).isFalse();
  }

  /** A function registered at run time (by an application or a plugin) is not a built-in the engine can vouch for. */
  @Test
  void aFunctionRegisteredAtRunTimeIsNotTrusted() {
    final String name = "issue8400runtimefn";
    DefaultSQLFunctionFactory.getInstance().register(new SQLFunctionAbstract(name) {
      @Override
      public Object execute(final Object self, final Identifiable currentRecord, final Object currentResult, final Object[] params,
          final CommandContext context) {
        return System.nanoTime();
      }

      @Override
      public String getSyntax() {
        return name + "()";
      }
    });
    try {
      assertThat(CorrelatedSubQueryCache.isCacheable(parse("select " + name + "() from CacheNode"))).isFalse();
    } finally {
      DefaultSQLFunctionFactory.getInstance().unregister(name);
    }
  }

  @Test
  void modificationCountMovesOnEveryLocalWritePath() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final long[] before = { db.getModificationCount() };
    final Runnable moved = () -> {
      final long now = db.getModificationCount();
      assertThat(now).isGreaterThan(before[0]);
      before[0] = now;
    };

    database.begin();
    final MutableDocument doc = database.newVertex("CacheNode").set("name", "Tmp").save();
    moved.run();
    doc.set("name", "Tmp2").save();
    moved.run();
    doc.delete();
    moved.run();
    database.commit();
    moved.run();

    database.transaction(() -> database.command("sql", "create vertex CacheNode set name = 'Tmp3'").close());
    moved.run();

    database.getSchema().createVertexType("Issue8400SchemaChange");
    moved.run();
  }

  @Test
  void copiesOfAParsedStatementCarryTheMemo() {
    final Statement statement = parse(ISSUE_QUERY);
    assertThat(CorrelatedSubQueryCache.isCacheable(statement)).isTrue();
    assertThat(statement.copy().resultCacheable).isTrue();
    assertThat(parse("traverse out() from CacheNode").copy().resultCacheable).isNull();
  }

  private Statement parse(final String sql) {
    return ((DatabaseInternal) database).getStatementCache().get(sql);
  }

  private static LetQueryStep rootLetStep(final ResultSet rs) {
    return findLetStep(rs, "root");
  }

  private static LetQueryStep findLetStep(final ResultSet rs, final String varName) {
    for (final ExecutionStep step : rs.getExecutionPlan().get().getSteps())
      if (step instanceof LetQueryStep letQueryStep && letQueryStep.prettyPrint(0, 0).contains("$" + varName + " = ("))
        return letQueryStep;
    throw new AssertionError("no LET $" + varName + " step in " + rs.getExecutionPlan().get().prettyPrint(0, 2));
  }
}
