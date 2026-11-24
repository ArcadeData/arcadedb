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
package com.arcadedb.query.opencypher;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13267, the rule rather than the cases: a differential fuzz of the reversal of a hop that starts from the node an index pins.
 * Each of {@value #SHAPES} generated statements of the form {@code [UNWIND / WITH ..] MATCH (e)-[r ..]->(n:Tgt {..}) [WHERE ..]
 * [WITH .. WHERE ..] RETURN ..} must answer the same rows, with the same multiplicity, as the same statement with the first node pinned
 * (an extra {@code e.uuid = e.uuid}), which makes the plan start from it as written. Where a reference sits (target map, relationship
 * map, inline relationship WHERE, node-pattern WHERE, outer WHERE, a later WITH, the RETURN list) and what it reads (the first node,
 * the relationship, the target, a bound name, a parameter, comprehensions, EXISTS and COUNT subqueries) vary, as do the direction, the
 * relationship types, the pins (equality, IN, OR, AND, a parameter, an UNWIND variable) and the indexes (NOTUNIQUE, UNIQUE, none). The
 * data has duplicates and nulls in the ids, parallel relationships, self-loops, a hub and a skewed key. The seed is fixed: the same
 * statements every run. {@code repros/labelled-source-reverse-hub-skew/reverse_fuzz.py} is the same generator against a server, with
 * any seed and count.
 */
class CypherLabelledSourceReverseFuzzIssue13267Test {
  private static final String PATH   = "./target/databases/cypher-labelled-source-reverse-fuzz-13267";
  private static final long   SEED   = 13267L;
  private static final int    SHAPES = 300;

  private static final Map<String, Object> PARAMS = Map.of("p", "t3", "q", "a",
      "ids", Arrays.asList("t0", "t3", "t3", null, "t5", "nosuch", "t7", "k2"));

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    for (final String type : List.of("Src", "Other", "Tgt"))
      database.command("sql", "CREATE VERTEX TYPE " + type);
    for (final String type : List.of("T", "T2"))
      database.command("sql", "CREATE EDGE TYPE " + type);
    for (final String property : List.of("Tgt.uuid", "Tgt.kid", "Tgt.grp", "Tgt.tag", "Src.uuid", "Src.tag"))
      database.command("sql", "CREATE PROPERTY " + property + " STRING");
    database.command("sql", "CREATE INDEX ON Tgt (uuid) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Tgt (kid) UNIQUE");
    database.command("sql", "CREATE INDEX ON Tgt (grp) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON Src (uuid) UNIQUE");

    final Random f = new Random(4242L);
    final List<Map<String, Object>> targets = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      final Map<String, Object> row = new HashMap<>();
      row.put("uuid", i == 8 ? "t7" : "t" + i); // t7 twice
      row.put("kid", "k" + i);
      row.put("grp", i % 3 != 0 ? "g0" : "g" + (1 + i % 5)); // one key on most of them
      row.put("num", i % 7);
      if (f.nextDouble() < 0.7)
        row.put("tag", f.nextBoolean() ? "a" : "b");
      targets.add(row);
    }
    final List<Map<String, Object>> sources = new ArrayList<>();
    for (int i = 0; i < 40; i++) {
      final Map<String, Object> row = new HashMap<>();
      row.put("uuid", "s" + i);
      row.put("num", i % 5);
      row.put("ref", "t" + f.nextInt(40));
      if (f.nextDouble() < 0.7)
        row.put("tag", f.nextBoolean() ? "a" : "b");
      sources.add(row);
    }
    database.transaction(() -> {
      database.command("opencypher", "UNWIND $rows AS row CREATE (n:Tgt) SET n = row", Map.of("rows", targets));
      database.command("opencypher", "UNWIND $rows AS row CREATE (n:Src) SET n = row", Map.of("rows", sources));
      database.command("opencypher", "UNWIND range(0, 7) AS i CREATE (:Other {uuid: 'o' + i, num: i % 4})");
      for (final String type : List.of("T", "T2")) {
        // Src to Tgt and Tgt to Src, parallel by chance, some with properties
        for (final String[] ends : new String[][] { { "Src", "Tgt" }, { "Tgt", "Src" } }) {
          final List<Map<String, Object>> rows = new ArrayList<>();
          for (int i = 0; i < 70; i++) {
            final Map<String, Object> row = new HashMap<>();
            final String s = "Src".equals(ends[0]) ? "s" + f.nextInt(40) : "t" + f.nextInt(40);
            final String e = "Src".equals(ends[0]) ? "t" + f.nextInt(40) : "s" + f.nextInt(40);
            row.put("s", s);
            row.put("e", e);
            if (f.nextDouble() < 0.6)
              row.put("w", f.nextInt(4));
            if (f.nextDouble() < 0.4)
              row.put("tag", f.nextBoolean() ? "a" : "b");
            rows.add(row);
          }
          database.command("opencypher", "UNWIND $rows AS row MATCH (a:" + ends[0] + " {uuid: row.s}), (b:" + ends[1] + " {uuid: row.e}) "
              + "CREATE (a)-[r:" + type + "]->(b) SET r.w = row.w, r.tag = row.tag", Map.of("rows", rows));
        }
        // Tgt to Tgt: ordinary, parallel, and self-loops
        final List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 0; i < 40; i++)
          rows.add(Map.of("s", "t" + f.nextInt(40), "e", "t" + f.nextInt(40), "w", f.nextInt(4)));
        for (final int loop : new int[] { 3, 9, 30 })
          rows.add(Map.of("s", "t" + loop, "e", "t" + loop, "w", 1));
        rows.addAll(new ArrayList<>(rows.subList(0, 8)));
        database.command("opencypher", "UNWIND $rows AS row MATCH (a:Tgt {uuid: row.s}), (b:Tgt {uuid: row.e}) CREATE (a)-[r:" + type
            + "]->(b) SET r.w = row.w", Map.of("rows", rows));
      }
      // a hub: t0 has 8 incoming relationships from the Other vertices and 3 from Src vertices
      database.command("opencypher", "MATCH (h:Tgt {uuid: 't0'}), (o:Other) UNWIND range(1, 12) AS k CREATE (o)-[:T {w: k % 3}]->(h)");
      database.command("opencypher", "MATCH (h:Tgt {uuid: 't0'}), (s:Src) WHERE s.uuid IN ['s1', 's2', 's3'] CREATE (s)-[:T {w: 1}]->(h)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void thePlannedStatementAnswersTheRowsOfTheWrittenOrder() {
    final Gen gen = new Gen(new Random(SEED));
    final Set<String> seen = new HashSet<>();
    final List<String> differences = new ArrayList<>();
    int shapes = 0;
    int reversed = 0;
    int withRows = 0;
    while (shapes < SHAPES) {
      final String text = gen.statement();
      if (!seen.add(text))
        continue;
      ++shapes;
      final List<String> planned = rowsOf(text);
      final List<String> written = rowsOf(block(text));
      if (planned == null && written == null)
        continue;
      if (planned == null || written == null || !planned.equals(written))
        differences.add(text + "\n   planned: " + (planned == null ? "error" : planned.size() + " rows " + abbreviate(planned)) + "\n   written: "
            + (written == null ? "error" : written.size() + " rows " + abbreviate(written)));
      else if (!planned.isEmpty())
        ++withRows;
      if (startsFromTheTarget(text))
        ++reversed;
    }
    assertThat(differences).as("statements whose rows differ from the written order").isEmpty();
    // the test must exercise the rule, not only the refusals
    assertThat(reversed).as("statements planned from the target").isGreaterThan(20);
    assertThat(withRows).as("statements that answered rows").isGreaterThan(100);
  }

  @Test
  void everyReferenceAtEverySiteAnswersTheRowsOfTheWrittenOrder() {
    // the random shapes meet a given reference at a given site rarely; this is the whole grid: every site (the target map, the
    // relationship map, the inline WHERE of the relationship, of the target and of the first node, the first node's map, the outer
    // WHERE) against every reference (the first node, the target, the relationship, a bound name, comprehensions, EXISTS, COUNT),
    // in each of the three directions
    final List<String[]> maps = List.of(new String[] { "tag", "e.tag" }, new String[] { "num", "e.num" }, new String[] { "tag", "n.tag" },
        new String[] { "num", "n.num" }, new String[] { "tag", "r.tag" }, new String[] { "w", "r.w" }, new String[] { "w", "e.num" },
        new String[] { "tag", "head([x IN [e.tag] | x])" }, new String[] { "tag", "head([(e)-[:T]->(m:Tgt) | m.tag])" },
        new String[] { "num", "COUNT { (e)-[:T]->() }" }, new String[] { "tag", "$q" });
    final List<String> predicates = List.of("e.tag = n.tag", "e.num = r.w", "n.num >= 3", "n.tag IS NOT NULL", "e.tag IS NULL", "r.w IS NOT NULL",
        "r.tag = e.tag", "EXISTS { MATCH (e)-[:T]->(:Tgt {uuid: 't1'}) }", "EXISTS { MATCH (n)-[:T]->(:Tgt {uuid: 't3'}) }",
        "COUNT { (n)-[:T]->() } > 1", "head([(e)-[:T]->(m:Tgt) | m.uuid]) IS NOT NULL", "id <> 't1'", "n.uuid = id", "e.uuid IS NOT NULL",
        "coalesce(e.tag, n.tag) = 'a'", "size([(n)-[:T2]->(m) | m]) >= 0");
    final List<String> statements = new ArrayList<>();
    for (final String arrow : List.of("-[REL]->", "<-[REL]-", "-[REL]-")) {
      for (final String[] map : maps) {
        final String ref = map[0] + ": " + map[1];
        statements.add(shape(arrow, "", "", ref, "", "", ""));   // target map
        statements.add(shape(arrow, "", ref, "", "", "", ""));   // relationship map
        statements.add(shape(arrow, "", "", "", "", ref, ""));   // first node map
      }
      for (final String predicate : predicates) {
        statements.add(shape(arrow, " WHERE " + predicate, "", "", "", "", ""));          // relationship WHERE
        statements.add(shape(arrow, "", "", "", " WHERE " + predicate, "", ""));          // target WHERE
        statements.add(shape(arrow, "", "", "", "", "", " WHERE " + predicate));          // first node WHERE
        statements.add(shape(arrow, "", "", "", "", "", "").replace(" RETURN ", " WHERE " + predicate + " RETURN ")); // outer WHERE
      }
    }
    final List<String> differences = new ArrayList<>();
    int reversed = 0;
    for (final String statement : statements) {
      final List<String> planned = rowsOf(statement);
      final List<String> written = rowsOf(block(statement));
      if (planned == null && written == null)
        continue;
      if (planned == null || written == null || !planned.equals(written))
        differences.add(statement + "\n   planned: " + (planned == null ? "error" : planned.size() + " rows") + "\n   written: "
            + (written == null ? "error" : written.size() + " rows"));
      if (startsFromTheTarget(statement))
        ++reversed;
    }
    assertThat(differences).as("statements whose rows differ from the written order").isEmpty();
    assertThat(reversed).as("statements planned from the target").isGreaterThan(40);
  }

  /** The skeleton of the grid: {@code arrow} holds the placeholder REL, which takes the relationship's own map and WHERE. */
  private static String shape(final String arrow, final String relWhere, final String relMap, final String targetMap, final String targetWhere,
      final String firstMap, final String firstWhere) {
    final String rel = "r:T" + (relMap.isEmpty() ? "" : " {" + relMap + "}") + relWhere;
    final String first = "(e:Src" + (firstMap.isEmpty() ? "" : " {" + firstMap + "}") + firstWhere + ")";
    final String target = "(n:Tgt {uuid: id" + (targetMap.isEmpty() ? "" : ", " + targetMap) + "}" + targetWhere + ")";
    return "UNWIND $ids AS id MATCH " + first + arrow.replace("REL", rel) + target + " RETURN e.uuid AS a, n.uuid AS b, id AS c, r.w AS d";
  }

  /** The rows as sorted text, so that two answers are equal when they hold the same rows the same number of times; null when it fails. */
  private List<String> rowsOf(final String query) {
    final List<String> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, PARAMS)) {
      while (rs.hasNext()) {
        final Result row = rs.next();
        final StringBuilder text = new StringBuilder();
        for (final String name : new TreeSet<>(row.getPropertyNames()))
          text.append(name).append('=').append((Object) row.getProperty(name)).append(';');
        rows.add(text.toString());
      }
    } catch (final RuntimeException e) {
      return null;
    }
    Collections.sort(rows);
    return rows;
  }

  private boolean startsFromTheTarget(final String query) {
    try (final ResultSet rs = database.query("opencypher", "EXPLAIN " + query, PARAMS)) {
      final String plan = String.valueOf((Object) rs.next().getProperty("executionPlanAsString"));
      if (plan.contains("Cost-Based"))
        return false;
      final int at = plan.indexOf("MATCH NODE (");
      return at >= 0 && plan.startsWith("n", at + "MATCH NODE (".length());
    } catch (final RuntimeException e) {
      return false;
    }
  }

  private static String abbreviate(final List<String> rows) {
    return rows.size() <= 4 ? rows.toString() : rows.subList(0, 4) + "...";
  }

  /**
   * The same statement with the first node pinned, so that a plan has to start from it: {@code e.uuid = e.uuid} AND-ed into the WHERE of
   * the MATCH (the first WHERE outside brackets after the pattern), or a new one before the later WITH or the RETURN.
   */
  static String block(final String text) {
    final int match = text.indexOf("MATCH ");
    final String head = text.substring(0, match);
    String body = text.substring(match + 5);
    final int cut = body.lastIndexOf(" RETURN ");
    final String ret = body.substring(cut);
    body = body.substring(0, cut);
    String later = "";
    final int with = body.indexOf(" WITH e");
    if (with >= 0) {
      later = body.substring(with);
      body = body.substring(0, with);
    }
    int depth = 0;
    int where = -1;
    for (int i = 0; i < body.length() && where < 0; i++) {
      final char c = body.charAt(i);
      if (c == '(' || c == '[' || c == '{')
        ++depth;
      else if (c == ')' || c == ']' || c == '}')
        --depth;
      else if (depth == 0 && body.startsWith(" WHERE ", i))
        where = i;
    }
    body = where >= 0 ? body.substring(0, where) + " WHERE (" + body.substring(where + 7) + ") AND e.uuid = e.uuid" : body + " WHERE e.uuid = e.uuid";
    return head + "MATCH" + body + later + ret;
  }

  /** The statement generator: about 70 percent of the shapes can be reversed, with references injected at random sites. */
  private static final class Gen {
    private static final List<String> LITERALS = List.of("'t1'", "'a'", "1", "null", "$p", "$q");

    private final Random random;

    Gen(final Random random) {
      this.random = random;
    }

    private boolean chance(final double p) {
      return random.nextDouble() < p;
    }

    private <X> X pick(final List<X> options) {
      return options.get(random.nextInt(options.size()));
    }

    private List<String> reads(final Set<String> scope) {
      final List<String> out = new ArrayList<>(LITERALS);
      if (scope.contains("e"))
        out.addAll(List.of("e.uuid", "e.tag", "e.num", "e.ref", "coalesce(e.tag, 'z')", "toString(e.num)", "head([x IN [e.uuid] | x])",
            "[x IN [e.tag, e.ref] WHERE x IS NOT NULL | x][0]", "head([(e)-[:T]->(m:Tgt) | m.uuid])", "size([(e)-[:T2]->(m) | m])",
            "COUNT { (e)-[:T]->() }", "COUNT { MATCH (e)-[:T2]->(m:Tgt) WHERE m.tag = 'a' }", "e.uuid + '|' + coalesce(e.tag, '')"));
      if (scope.contains("r"))
        out.addAll(List.of("r.w", "r.tag", "coalesce(r.w, -1)", "r.w + 1", "head([x IN [r.tag] | x])"));
      if (scope.contains("n"))
        out.addAll(List.of("n.uuid", "n.tag", "n.grp", "n.num", "n.kid", "head([(n)-[:T]->(m) | m.uuid])", "COUNT { (n)-[:T]->() }"));
      if (scope.contains("id"))
        out.addAll(List.of("id", "id", "toString(id)", "id + 'x'", "coalesce(id, 0)"));
      if (scope.contains("ids"))
        out.addAll(List.of("head(ids)", "size(ids)"));
      if (scope.contains("w"))
        out.addAll(List.of("w", "w + 1"));
      return out;
    }

    private List<String> booleanReads(final Set<String> scope) {
      final List<String> out = new ArrayList<>();
      if (scope.contains("e"))
        out.addAll(List.of("EXISTS { MATCH (e)-[:T]->(:Tgt {uuid: 't1'}) }", "EXISTS { (e)-[:T2]->(:Tgt) }", "COUNT { (e)-[:T]->() } > 1",
            "e.tag IS NULL", "e.num > 1"));
      if (scope.contains("n"))
        out.addAll(List.of("EXISTS { MATCH (n)-[:T]->(:Tgt {uuid: 't3'}) }", "n.tag IS NOT NULL", "n.num >= 3"));
      if (scope.contains("r"))
        out.addAll(List.of("r.w IS NOT NULL", "r.w > 1", "r.tag = $q"));
      if (scope.contains("id"))
        out.addAll(List.of("id IS NOT NULL", "id <> 't1'"));
      return out;
    }

    private String predicate(final Set<String> scope) {
      return predicate(scope, null);
    }

    private String predicate(final Set<String> scope, final String forced) {
      final List<String> all = reads(scope);
      final double k = random.nextDouble();
      if (k < 0.15 && !booleanReads(scope).isEmpty())
        return pick(booleanReads(scope));
      List<String> focus = new ArrayList<>();
      for (final String v : List.of("e", "n", "r", "id"))
        if (scope.contains(v))
          focus.add(v);
      final String a;
      if (!focus.isEmpty() && (forced != null && scope.contains(forced) || chance(0.6))) { // a predicate about one variable
        if (forced != null && scope.contains(forced))
          focus = List.of(forced);
        final List<String> own = new ArrayList<>(reads(Set.of(pick(focus))));
        own.removeAll(LITERALS);
        a = pick(own);
      } else
        a = pick(all);
      if (k < 0.30)
        return a + " IS NOT NULL";
      if (k < 0.40)
        return a + " IN [" + pick(all) + ", " + pick(all) + "]";
      final List<String> variables = new ArrayList<>(all);
      variables.removeAll(LITERALS);
      final String right = chance(0.4) || variables.isEmpty() ? pick(all) : pick(variables);
      return a + " " + pick(List.of("=", "=", "=", "<>")) + " " + right;
    }

    /** A reference to put where only what an earlier clause bound may be read: mostly the first node, the target or the relationship. */
    private String injected(final Set<String> scope) {
      final String v = pick(Arrays.asList("e", "e", "e", "n", "n", "r", "id", null));
      if (v == null || !scope.contains(v))
        return pick(reads(scope));
      final List<String> own = new ArrayList<>(reads(Set.of(v)));
      own.removeAll(LITERALS);
      return pick(own);
    }

    private String key(final Set<String> scope) {
      final double k = random.nextDouble();
      if (scope.contains("id") && k < 0.5)
        return pick(List.of("id", "id", "toString(id)"));
      if (k < 0.65)
        return pick(List.of("'t1'", "'t3'", "'t0'", "'k4'", "'g0'", "'a'", "$p", "$q", "'nosuch'"));
      if (k < 0.75 && scope.contains("ids"))
        return "head(ids)";
      return pick(reads(scope));
    }

    private String keyList(final Set<String> scope) {
      final double k = random.nextDouble();
      if (scope.contains("ids") && k < 0.3)
        return "ids";
      if (k < 0.55)
        return "$ids";
      if (k < 0.65 && scope.contains("id"))
        return "[id, id]";
      final StringBuilder list = new StringBuilder("[");
      final int n = 1 + random.nextInt(4);
      for (int i = 0; i < n; i++)
        list.append(i == 0 ? "" : ", ").append(pick(List.of("'t0'", "'t1'", "'t3'", "'t7'", "'k2'", "'g0'", "'nosuch'", "null", "$p")));
      return list.append("]").toString();
    }

    String statement() {
      final boolean friendly = chance(0.7);
      final List<Object[]> prefixes = new ArrayList<>(List.of(
          new Object[] { "", Set.of() },
          new Object[] { "UNWIND $ids AS id ", Set.of("id") }, new Object[] { "UNWIND $ids AS id ", Set.of("id") },
          new Object[] { "UNWIND ['t1', 't3', null, 't3', 'nosuch', 't0'] AS id ", Set.of("id") },
          new Object[] { "WITH 1 AS one ", Set.of() },
          new Object[] { "WITH $ids AS ids UNWIND ids AS id ", Set.of("id", "ids") },
          new Object[] { "WITH 't3' AS id ", Set.of("id") },
          new Object[] { "UNWIND $ids AS id WITH id, 1 AS w ", Set.of("id", "w") },
          new Object[] { "UNWIND $ids AS id WITH id WHERE id IS NOT NULL ", Set.of("id") },
          new Object[] { "UNWIND ['t0', 't9'] AS id UNWIND [1, 2] AS w ", Set.of("id", "w") },
          new Object[] { "UNWIND ['t1', 't3'] AS id WITH 't0' AS id ", Set.of("id") },
          new Object[] { "UNWIND $ids AS id WITH id AS id ", Set.of("id") },
          new Object[] { "WITH $ids AS ids UNWIND ids AS id WITH id, ids ", Set.of("id", "ids") }));
      if (friendly)
        prefixes.removeIf(p -> !((Set<?>) p[1]).contains("id"));
      final Object[] chosen = pick(prefixes);
      final String prefix = (String) chosen[0];
      @SuppressWarnings("unchecked") final Set<String> bound = (Set<String>) chosen[1];

      final String firstKind = friendly ? pick(List.of("Src", "Src", "none")) : pick(List.of("Src", "Src", "Src", "none", "Tgt", "Other"));
      String first = switch (firstKind) {
        case "Src" -> "(e:Src";
        case "none" -> "(e";
        case "Tgt" -> "(e:Tgt";
        default -> "(e:Other";
      };
      final boolean namedRel = chance(friendly ? 0.9 : 0.8);
      final Set<String> scope = new LinkedHashSet<>(bound);
      scope.add("e");
      scope.add("n");
      if (namedRel)
        scope.add("r");
      final Set<String> sites = new HashSet<>();
      if (friendly) {
        final List<String> all = new ArrayList<>(List.of("tmap", "rprops", "rwhere", "twhere", "fwhere", "fmap", "ow", "later", "ret"));
        Collections.shuffle(all, random);
        sites.addAll(all.subList(0, pick(List.of(1, 1, 2, 3))));
        if (chance(0.6)) // most friendly shapes probe the sites the wrong results sat at: target map, relationship map and WHERE
          sites.add(pick(List.of("tmap", "rprops", "rwhere")));
      }
      String firstExtra = "";
      if (sites.contains("fmap") || !friendly && chance(0.12))
        firstExtra = " {" + pick(List.of("num: 1", "tag: " + key(scope), "uuid: " + key(scope))) + "}";
      if (sites.contains("fwhere") || !friendly && chance(0.1))
        firstExtra = " WHERE " + predicate(scope);
      first += firstExtra + ")";

      final String types = friendly ? pick(List.of(":T", ":T2", ":T|T2")) : pick(List.of(":T", ":T", ":T2", "", ":T|T2", ":T"));
      final List<String> props = new ArrayList<>();
      if (sites.contains("rprops") || !friendly && chance(0.25)) {
        final String x = friendly ? injected(scope) : key(scope);
        props.add((x.contains("tag") ? "tag" : x.contains(".w") ? "w" : pick(List.of("w", "tag"))) + ": " + x);
      }
      final String relWhere = sites.contains("rwhere") || !friendly && chance(0.25)
          ? " WHERE " + predicate(scope, pick(List.of("n", "n", "e", "r", "id"))) : "";
      final String rel = (namedRel ? "r" : "") + types + (props.isEmpty() ? "" : " {" + String.join(", ", props) + "}") + relWhere;
      final String direction = pick(friendly ? List.of("->", "->", "<-", "-") : List.of("->", "->", "->", "<-", "-"));
      final String relPattern = switch (direction) {
        case "->" -> "-[" + rel + "]->";
        case "<-" -> "<-[" + rel + "]-";
        default -> "-[" + rel + "]-";
      };

      final String targetLabel = friendly ? ":Tgt" : pick(List.of(":Tgt", ":Tgt", ":Tgt", ":Tgt", ""));
      final List<String> targetMap = new ArrayList<>();
      final List<String> outer = new ArrayList<>();
      final int pins = friendly ? 1 + random.nextInt(2) : random.nextInt(4);
      for (int i = 0; i < pins; i++) {
        final String property = i == 0 ? pick(List.of("uuid", "uuid", "kid", "grp")) : pick(List.of("uuid", "uuid", "kid", "grp", "tag", "num"));
        final String x = friendly && i == 0 ? pick(List.of("id", "id", "id", "$p", "'t3'", "toString(id)")) : key(scope);
        final double form = random.nextDouble();
        if (form < 0.5)
          targetMap.add(property + ": " + x);
        else if (form < 0.65)
          outer.add("n." + property + " = " + x);
        else if (form < 0.8)
          outer.add("n." + property + " IN " + keyList(scope));
        else if (form < 0.9)
          outer.add("(n." + property + " = " + x + " OR n." + property + " = " + key(scope) + ")");
        else
          outer.add("(n." + property + " IN " + keyList(scope) + " AND n." + pick(List.of("tag", "num")) + " = " + key(scope) + ")");
      }
      if (sites.contains("tmap")) {
        final String x = injected(scope);
        targetMap.add((x.contains("tag") ? "tag" : x.contains("num") ? "num" : pick(List.of("tag", "num"))) + ": " + x);
      }
      final String targetWhere = sites.contains("twhere") || !friendly && chance(0.12)
          ? " WHERE " + predicate(scope, pick(List.of("e", "r", "id", "n"))) : "";
      final String target = "(n" + targetLabel + (targetMap.isEmpty() ? "" : " {" + String.join(", ", targetMap) + "}") + targetWhere + ")";
      if (sites.contains("ow") || !friendly && chance(0.35))
        outer.add(predicate(scope));
      if (!friendly && chance(0.1))
        outer.add(predicate(scope));
      final String where = outer.isEmpty() ? "" : " WHERE " + String.join(" AND ", outer);
      final String comma = chance(0.07) ? ", (o:Other)-[:T]->(n)" : "";

      String later = "";
      Set<String> keep = scope;
      if (sites.contains("later") || !friendly && chance(0.15)) {
        keep = new LinkedHashSet<>(List.of("e", "n"));
        if (namedRel)
          keep.add("r");
        if (bound.contains("id"))
          keep.add("id");
        if (bound.contains("ids"))
          keep.add("ids");
        later = " WITH " + String.join(", ", keep) + " WHERE " + predicate(keep);
      }
      final List<String> items = new ArrayList<>(List.of("e.uuid AS a", "n.uuid AS b"));
      if (bound.contains("id"))
        items.add("id AS c");
      if (namedRel)
        items.add("r.w AS d");
      if (sites.contains("ret") || !friendly && chance(0.3))
        items.add(pick(reads(keep)) + " AS x");
      final String ret = pick(List.of("RETURN " + String.join(", ", items), "RETURN " + String.join(", ", items), "RETURN count(*) AS c",
          "RETURN count(DISTINCT n) AS c, count(DISTINCT e) AS d"));
      return prefix + "MATCH " + first + relPattern + target + comma + where + later + " " + ret;
    }
  }
}
