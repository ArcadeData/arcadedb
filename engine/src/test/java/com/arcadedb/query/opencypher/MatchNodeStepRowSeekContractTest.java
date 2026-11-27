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

import com.arcadedb.TestHelper;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the batched read shapes of the modelhub and Graphiti clients (an UNWIND of keys or record ids feeding a MATCH) must answer
 * from {@code MatchNodeStep}: the rows a brute-force walk of the data selects, and the way the step reaches them (a direct fetch
 * by record id, an index seek, a prefix range, a transient hash of an unindexed property), which the PROFILE plan names and whose
 * candidate count it reports. A step that quietly scanned the type per input row would answer the same rows, so the plan and the
 * candidate count are part of what is pinned; the clients document that shape as the only one that is not a scan.
 * <p>
 * The oracle is the data itself: every expected row is computed here from the records the test wrote, never from another query.
 */
class MatchNodeStepRowSeekContractTest extends TestHelper {
  private static final int          CHUNKS      = 20;
  private static final int          PLAIN       = 120;
  private static final String       OPTIONAL    = " OPTIONAL MATCH (n)-[:LINKS]->(zz)";
  private static final Pattern      MATCH_ROWS  = Pattern.compile("MATCH NODE \\(n[^\\n]*?(\\d[\\d,]*) rows");

  private final List<String>        chunkRids   = new ArrayList<>();

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final VertexType chunk = database.getSchema().createVertexType("Chunk");
      chunk.createProperty("external_id", String.class);
      chunk.createProperty("seq", Integer.class);
      chunk.createProperty("flag", Boolean.class);
      chunk.createProperty("code", String.class);
      chunk.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "external_id");
      chunk.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "code");
      chunk.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "seq");

      final VertexType plain = database.getSchema().createVertexType("Plain");
      plain.createProperty("key", String.class);
      plain.createProperty("num", Integer.class);

      final VertexType comp = database.getSchema().createVertexType("Comp");
      comp.createProperty("tenant", String.class);
      comp.createProperty("ext", String.class);
      comp.createProperty("n", Integer.class);
      comp.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "tenant", "ext");

      database.getSchema().createVertexType("Other").createProperty("external_id", String.class);
      database.getSchema().createEdgeType("LINKS");
    });

    database.transaction(() -> {
      for (int i = 0; i < CHUNKS; i++) {
        final MutableDocument v = database.newVertex("Chunk").set("external_id", "e" + i).set("seq", i).set("flag", i % 2 == 0)
            .set("code", "c" + (i % 5)).save();
        chunkRids.add(v.getIdentity().toString());
      }
      database.newVertex("Other").set("external_id", "e0").save();

      for (int i = 0; i < PLAIN; i++)
        database.newVertex("Plain").set("key", "k" + i).set("num", i).save();

      for (int t = 0; t < 3; t++)
        for (int x = 0; x < 10; x++)
          database.newVertex("Comp").set("tenant", "t" + t).set("ext", "x" + x).set("n", t * 10 + x).save();
    });
  }

  // ---- the direct fetch by record id: UNWIND $rids AS rid MATCH (n:L) WHERE id(n) = rid [AND ...] ----------------------------

  @Test
  void idFetchPerRowAnswersTheRowsAndNamesTheDirectFetch() {
    final List<Object> rids = Arrays.asList(chunkRids.get(3), chunkRids.get(7), chunkRids.get(12));
    final Map<String, Object> params = Map.of("rids", rids);

    for (final String where : List.of("id(n) = rid", "rid = id(n)", "elementId(n) = rid", "rid = elementId(n)")) {
      final String query = "UNWIND $rids AS rid MATCH (n:Chunk) WHERE " + where + " RETURN rid, n.seq AS seq";
      assertThat(seqs(query, params)).as(where).containsExactly(3, 7, 12);

      final String plan = plan(query, params);
      assertThat(plan).as(where).contains("MATCH NODE (n:Chunk) [id: per-row]");
      assertThat(candidates(plan)).as(where + ": one record fetched per id, no scan of the type").isEqualTo(3);
    }
  }

  @Test
  void idFetchPerRowKeepsTheOtherConjunctsInEitherOrder() {
    final List<Object> rids = new ArrayList<>(chunkRids.subList(0, 10));
    final Map<String, Object> params = Map.of("rids", rids);

    // seq > 5 AND flag (even seq) over ids 0..9: 6 and 8
    final List<String> shapes = List.of(
        "id(n) = rid AND n.seq > 5 AND n.flag = true",
        "n.seq > 5 AND id(n) = rid AND n.flag = true",
        "n.seq > 5 AND n.flag = true AND id(n) = rid",
        "n.flag = true AND (n.seq > 5 AND id(n) = rid)",
        "(id(n) = rid AND n.seq > 5) AND n.flag = true",
        "rid = id(n) AND n.seq > 5 AND n.flag = true");
    for (final String where : shapes) {
      final String query = "UNWIND $rids AS rid MATCH (n:Chunk) WHERE " + where + " RETURN rid, n.seq AS seq";
      assertThat(seqs(query, params)).as(where).containsExactly(6, 8);
      assertThat(seqs(query + " ORDER BY seq DESC", params)).as(where + " (ordered)").containsExactly(8, 6);

      final String plan = plan(query, params);
      assertThat(plan).as(where).contains("[id: per-row]");
      assertThat(candidates(plan)).as(where + ": one record fetched per id").isEqualTo(10);
    }
  }

  @Test
  void idFetchPerRowKeepsEveryConjunctThatIsNotTheIdEquality() {
    // the equality is answered by the fetch and must leave the filter; what stays is exactly the rest
    final Map<String, Object> params = Map.of("rids", new ArrayList<Object>(chunkRids.subList(0, 4)));
    final String plan = plan("UNWIND $rids AS rid MATCH (n:Chunk) WHERE n.seq > 1 AND id(n) = rid AND n.flag = true RETURN n.seq AS seq",
        params);
    final String matchLine = Arrays.stream(plan.split("\n")).filter(l -> l.contains("MATCH NODE (n:Chunk)")).findFirst().orElseThrow();
    assertThat(matchLine).contains("[filter: ").contains("n.seq").contains("n.flag").doesNotContain("id(n)");
  }

  @Test
  void idFetchPerRowTakesTheRidFromAMapField() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    rows.add(Map.of("rid", chunkRids.get(2), "min", 0));
    rows.add(Map.of("rid", chunkRids.get(5), "min", 9));
    rows.add(new HashMap<>());
    rows.add(Map.of("rid", chunkRids.get(9), "min", 0));
    final Map<String, Object> params = Map.of("rows", rows);

    final String query = "UNWIND $rows AS row MATCH (n:Chunk) WHERE id(n) = row.rid AND n.seq >= row.min RETURN n.seq AS seq";
    assertThat(seqs(query, params)).containsExactly(2, 9);
    assertThat(seqs(withOptional(query), params)).containsExactly(2, 9);
  }

  @Test
  void idFetchPerRowAnswersNoRowForANullInvalidOrMissingId() {
    final List<Object> rids = new ArrayList<>();
    rids.add(null);
    rids.add("not-a-rid");
    rids.add("");
    rids.add("#99:99");
    rids.add(chunkRids.get(1));
    final Map<String, Object> params = Map.of("rids", rids);

    for (final String suffix : List.of("", OPTIONAL)) {
      final String query = "UNWIND $rids AS rid MATCH (n:Chunk) WHERE id(n) = rid" + suffix + " RETURN rid, n.seq AS seq";
      assertThat(seqs(query, params)).as(suffix).containsExactly(1);
    }
  }

  @Test
  void idFetchPerRowAcceptsTheNumericIdAndTheNodeItself() {
    final List<Object> numeric = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", "MATCH (n:Chunk) WHERE n.seq IN [4, 11] RETURN id(n) AS i ORDER BY n.seq")) {
      while (rs.hasNext())
        numeric.add(rs.next().getProperty("i"));
    }
    assertThat(numeric).hasSize(2);
    final String query = "UNWIND $ids AS i MATCH (n:Chunk) WHERE id(n) = i RETURN i, n.seq AS seq";
    assertThat(seqs(query, Map.of("ids", numeric))).containsExactly(4, 11);
    assertThat(plan(query, Map.of("ids", numeric))).contains("[id: per-row]");

    // a node bound earlier is its own id: MATCH (m) ... MATCH (n) WHERE id(n) = id(m)
    final String viaNode = "MATCH (m:Chunk {external_id: 'e6'}) WITH m MATCH (n:Chunk) WHERE id(n) = id(m) RETURN n.seq AS seq";
    assertThat(seqs(viaNode, Map.of())).containsExactly(6);
  }

  @Test
  void idEqualityOrAnotherPredicateIsNotAFetch() {
    final Map<String, Object> params = Map.of("rids", new ArrayList<Object>(List.of(chunkRids.get(1))));
    final String query = "UNWIND $rids AS rid MATCH (n:Chunk) WHERE id(n) = rid OR n.seq = 15 RETURN n.seq AS seq";
    assertThat(seqs(query, params)).containsExactlyInAnyOrder(1, 15);
    assertThat(plan(query, params)).doesNotContain("[id: per-row]");
  }

  // ---- per-row candidates through an index, or through none ---------------------------------------------------------------------

  @Test
  void aValueTheRowDecidesIsSoughtForEveryRow() {
    final List<Object> eids = Arrays.asList("e3", "e7", null, "missing", "e3", "e0");
    final Map<String, Object> params = Map.of("eids", eids);
    final List<Integer> expected = List.of(3, 7, 3, 0);

    final List<String> shapes = List.of(
        "UNWIND $eids AS eid MATCH (n:Chunk {external_id: eid}) RETURN eid, n.seq AS seq",
        "UNWIND $eids AS eid MATCH (n:Chunk) WHERE n.external_id = eid RETURN eid, n.seq AS seq",
        "UNWIND $eids AS eid MATCH (n:Chunk) WHERE eid = n.external_id RETURN eid, n.seq AS seq",
        "UNWIND $eids AS eid MATCH (n:Chunk {external_id: eid}) WHERE n.seq >= 0 RETURN eid, n.seq AS seq",
        "UNWIND $eids AS eid MATCH (n:Chunk) WHERE n.seq >= 0 AND n.external_id = eid RETURN eid, n.seq AS seq");
    for (final String query : shapes) {
      assertThat(seqs(query, params)).as(query).containsExactlyElementsOf(expected);
      assertThat(seqs(query.replace(" RETURN", OPTIONAL + " RETURN"), params)).as(query + " + optional").containsExactlyElementsOf(expected);
      assertThat(plan(query, params)).as(query).contains("[index: Chunk[external_id]]");
    }
  }

  @Test
  void anEqualityOnTheSameValueForEveryRowIsAnswered() {
    // the value is the same for every row: its candidates are the same for every row, and still the right ones
    final Map<String, Object> params = Map.of("xs", List.of(1, 2, 3), "eid", "e5");
    assertThat(seqs("UNWIND $xs AS x MATCH (n:Chunk {external_id: $eid}) RETURN x, n.seq AS seq", params)).containsExactly(5, 5, 5);
    assertThat(seqs("UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id = $eid RETURN x, n.seq AS seq", params)).containsExactly(5, 5, 5);
    assertThat(seqs("UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id = 'e5' RETURN x, n.seq AS seq", params)).containsExactly(5, 5, 5);
  }

  @Test
  void aListTheRowDecidesIsSoughtForEveryRow() {
    final List<Object> lists = new ArrayList<>();
    lists.add(Arrays.asList("e1", "e2", "e1", null));
    lists.add(Collections.emptyList());
    lists.add(Arrays.asList("e9", "nope"));
    lists.add(Arrays.asList("e2", "e3"));
    final Map<String, Object> params = Map.of("lists", lists);

    final String query = "UNWIND $lists AS l MATCH (n:Chunk) WHERE n.external_id IN l RETURN n.seq AS seq";
    final List<Integer> expected = List.of(1, 2, 9, 2, 3);
    assertThat(seqs(withOptional(query), params)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(seqs(query, params)).containsExactlyInAnyOrderElementsOf(expected);

    // the same lists over a property no index covers: same rows, whatever the way they are found
    final String unindexed = "UNWIND $lists AS l MATCH (n:Plain) WHERE n.key IN l RETURN n.num AS seq";
    final List<Object> plainLists = new ArrayList<>();
    plainLists.add(Arrays.asList("k4", "k5", null));
    plainLists.add(Collections.emptyList());
    plainLists.add(Arrays.asList("k5", "k100", "none"));
    assertThat(seqs(unindexed, Map.of("lists", plainLists))).containsExactlyInAnyOrder(4, 5, 5, 100);
  }

  @Test
  void aFixedListSeeksTheIndexOnceAndTheRowsAllGetItsAnswer() {
    final Map<String, Object> params = Map.of("xs", List.of(1, 2), "ids", List.of("e4", "e5", "e4", "zzz"));
    final String query = "UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id IN $ids RETURN x, n.seq AS seq";
    assertThat(seqs(query, params)).containsExactlyInAnyOrder(4, 5, 4, 5);
    assertThat(seqs(query + " ORDER BY x, seq", params)).containsExactly(4, 5, 4, 5);
  }

  @Test
  void theInListOfTheFewestKeysIsTheOneSought() {
    final List<String> many = new ArrayList<>();
    for (int i = 0; i < 12; i++)
      many.add("e" + i);
    final Map<String, Object> params = Map.of("many", many, "few", List.of("c1", "c2"), "xs", List.of(1));

    // external_id IN (12 keys) AND code IN (2 keys): the index with 2 keys is the cheaper seek
    final String fewer = "UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id IN $many AND n.code IN $few RETURN n.seq AS seq";
    assertThat(plan(fewer, params)).contains("[index: Chunk[code]]");
    assertThat(seqs(fewer, params)).containsExactlyInAnyOrder(1, 2, 6, 7, 11);

    final String fewerFirst = "UNWIND $xs AS x MATCH (n:Chunk) WHERE n.code IN $few AND n.external_id IN $many RETURN n.seq AS seq";
    assertThat(plan(fewerFirst, params)).contains("[index: Chunk[code]]");
    assertThat(seqs(fewerFirst, params)).containsExactlyInAnyOrder(1, 2, 6, 7, 11);

    // equally long lists: the first one written is the one sought
    final Map<String, Object> equal = Map.of("a", List.of("e1", "e2"), "b", List.of("c1", "c2"), "xs", List.of(1));
    final String tie = "UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id IN $a AND n.code IN $b RETURN n.seq AS seq";
    assertThat(plan(tie, equal)).contains("[index: Chunk[external_id]]");
    assertThat(seqs(tie, equal)).containsExactlyInAnyOrder(1, 2);
  }

  @Test
  void anInListWithAnElementThatIsNotAScalarIsLeftToTheFilter() {
    // a list inside the list can only equal a list: no key of the index, and the filter says it matches nothing here
    final List<Object> mixed = new ArrayList<>();
    mixed.add("e1");
    mixed.add(List.of("e2"));
    final Map<String, Object> params = Map.of("xs", List.of(1), "ids", mixed);
    assertThat(seqs("UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id IN $ids RETURN n.seq AS seq", params)).containsExactly(1);
    assertThat(seqs("UNWIND $xs AS x MATCH (n:Chunk) WHERE n.external_id IN $ids RETURN n.seq AS seq" + "", Map.of("xs", List.of(1),
        "ids", Collections.singletonList(null)))).isEmpty();
  }

  @Test
  void anInListMayBeWrittenAsElementsThatReadTheRow() {
    final Map<String, Object> params = Map.of("rows", List.of(Map.of("a", "e1", "b", "e2"), Map.of("a", "e3", "b", "e3")));
    final String query = "UNWIND $rows AS r MATCH (n:Chunk) WHERE n.external_id IN [r.a, r.b] RETURN n.seq AS seq";
    assertThat(seqs(query, params)).containsExactlyInAnyOrder(1, 2, 3);
  }

  // ---- an unindexed property of a chained match: the transient hash ---------------------------------------------------------

  @Test
  void anUnindexedInlineEqualityOfAChainedMatchIsAnsweredFromTheHash() {
    final List<Object> keys = new ArrayList<>();
    final List<Integer> expected = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      keys.add("k" + (i * 7));
      expected.add(i * 7);
    }
    keys.add(null);
    keys.add("absent");
    final Map<String, Object> params = Map.of("keys", keys);

    final String query = "UNWIND $keys AS k MATCH (n:Plain {key: k}) RETURN k, n.num AS seq";
    assertThat(seqs(query, params)).containsExactlyElementsOf(expected);
    // 12 matches among 120 records: the first row scans the type, the second builds the hash, and the others visit their own
    // match only, where a scan per row would visit 120 records for each of the 14 keys
    assertThat(candidates(plan(query, params))).isLessThan(2 * PLAIN);
  }

  @Test
  void aHashOfFewDistinctValuesIsNotUsedAndTheScanAnswers() {
    // 'num % 4' has 4 distinct values over 120 records: not selective, so each row is answered by the scan
    final VertexType plain = database.getSchema().getOrCreateVertexType("Few");
    database.transaction(() -> {
      for (int i = 0; i < PLAIN; i++)
        database.newVertex(plain.getName()).set("grp", "g" + (i % 4)).set("num", i).save();
    });
    final Map<String, Object> params = Map.of("gs", List.of("g0", "g3", "g9"));
    assertThat(seqs("UNWIND $gs AS g MATCH (n:Few {grp: g}) RETURN g, n.num AS seq", params)).hasSize(60);
    assertThat(seqs("UNWIND $gs AS g MATCH (n:Few {grp: g}) RETURN g, n.num AS seq ORDER BY seq", params).get(0)).isEqualTo(0);
  }

  @Test
  void aHashSeesNumbersTheWayTheScanDoes() {
    database.transaction(() -> {
      final VertexType mixed = database.getSchema().getOrCreateVertexType("Mixed");
      for (int i = 0; i < 70; i++)
        database.newVertex(mixed.getName()).set("v", (long) i).set("id", i).save();
      database.newVertex(mixed.getName()).set("v", 3.0d).set("id", 1003).save();
      database.newVertex(mixed.getName()).set("v", 7.5d).set("id", 1007).save();
      database.newVertex(mixed.getName()).set("v", "9").set("id", 1009).save();
      database.newVertex(mixed.getName()).set("id", 1010).save(); // no v at all
    });
    final List<Object> probes = new ArrayList<>();
    probes.add(3);
    probes.add(3L);
    probes.add(3.0d);
    probes.add(7.5d);
    probes.add("9");
    probes.add(9);
    probes.add(null);
    probes.add(1000);
    final List<Integer> perValue = new ArrayList<>();
    for (final Object probe : probes) {
      final Map<String, Object> single = new HashMap<>();
      single.put("p", probe);
      perValue.addAll(seqs("MATCH (n:Mixed {v: $p}) RETURN $p AS p, n.id AS seq ORDER BY seq", single));
    }
    final Map<String, Object> params = Map.of("probes", probes);
    assertThat(seqs("UNWIND $probes AS p MATCH (n:Mixed {v: p}) RETURN p, n.id AS seq", params)).containsExactlyInAnyOrderElementsOf(perValue);
  }

  // ---- a prefix of a composite index ------------------------------------------------------------------------------------------

  @Test
  void aLeadingPrefixOfACompositeIndexIsARangeSeekPerRow() {
    final List<Object> tenants = Arrays.asList("t0", "t2", null, "t9", "t1");
    final Map<String, Object> params = Map.of("ts", tenants);

    final String query = "UNWIND $ts AS t MATCH (n:Comp {tenant: t}) RETURN t, n.n AS seq";
    final List<Integer> expected = new ArrayList<>();
    for (final int tenant : new int[] { 0, 2, 1 })
      for (int x = 0; x < 10; x++)
        expected.add(tenant * 10 + x);
    assertThat(seqs(query, params)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(plan(query, params)).contains("[index: Comp[tenant, ext], prefix: tenant]");

    final String whole = "UNWIND $ts AS t MATCH (n:Comp {tenant: t, ext: 'x4'}) RETURN t, n.n AS seq";
    assertThat(seqs(whole, params)).containsExactlyInAnyOrder(4, 24, 14);
    assertThat(plan(whole, params)).contains("[index: Comp[tenant, ext]]").doesNotContain("prefix");

    final String fromWhere = "UNWIND $ts AS t MATCH (n:Comp) WHERE n.tenant = t RETURN t, n.n AS seq";
    assertThat(seqs(fromWhere, params)).containsExactlyInAnyOrderElementsOf(expected);
    assertThat(plan(fromWhere, params)).contains("prefix: tenant");
  }

  @Test
  void aPrefixThatIsNotTheLeadingPropertyIsAScan() {
    final Map<String, Object> params = Map.of("xs", List.of("x1", "x8"));
    final String query = "UNWIND $xs AS x MATCH (n:Comp {ext: x}) RETURN x, n.n AS seq";
    assertThat(seqs(query, params)).containsExactlyInAnyOrder(1, 11, 21, 8, 18, 28);
    assertThat(plan(query, params)).doesNotContain("[index:");
  }

  @Test
  void aPrefixOrAKeyListPastTheReadWholeLimitStreamsTheRestOnce() {
    database.transaction(() -> {
      final VertexType big = database.getSchema().createVertexType("Big");
      big.createProperty("tenant", String.class);
      big.createProperty("ext", String.class);
      big.createProperty("k", Integer.class);
      big.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "tenant", "ext");
      big.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "k");
      for (int i = 0; i < 600; i++)
        database.newVertex("Big").set("tenant", "big").set("ext", "x" + i).set("k", i % 300).save();
      for (int i = 0; i < 40; i++)
        database.newVertex("Big").set("tenant", "small").set("ext", "x" + i).set("k", 1000 + i).save();
    });

    // 600 records under one prefix: more than the entries read whole, the rest streams, and every record is answered once
    assertThat(seqs("UNWIND ['big', 'small'] AS t MATCH (n:Big {tenant: t}) RETURN t, count(n) AS seq ORDER BY t", Map.of()))
        .containsExactly(600, 40);
    assertThat(seqs("UNWIND ['big'] AS t MATCH (n:Big {tenant: t}) RETURN count(DISTINCT id(n)) AS seq", Map.of())).containsExactly(600);
    assertThat(plan("UNWIND ['big'] AS t MATCH (n:Big {tenant: t}) RETURN n.k AS seq", Map.of())).contains("prefix: tenant");
    assertThat(seqs("UNWIND ['big'] AS t MATCH (n:Big {tenant: t}) RETURN n.k AS seq LIMIT 3", Map.of())).hasSize(3);

    // an IN list of 600 elements over a non unique index: each of the 300 keys twice in the list, two records each
    final List<Object> keys = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      keys.add(i);
      keys.add(i);
    }
    keys.add(null);
    keys.add(5000);
    final Map<String, Object> params = Map.of("ks", keys);
    final List<Integer> answered = seqs("UNWIND [1] AS x MATCH (n:Big) WHERE n.k IN $ks RETURN n.k AS seq", params);
    assertThat(answered).hasSize(600);
    assertThat(answered.stream().distinct().count()).isEqualTo(300);
    assertThat(seqs("UNWIND [1] AS x MATCH (n:Big) WHERE n.k IN $ks RETURN count(DISTINCT id(n)) AS seq", params)).containsExactly(600);
    assertThat(seqs("UNWIND [1] AS x MATCH (n:Big) WHERE n.k IN $ks RETURN n.k AS seq LIMIT 5", params)).hasSize(5);
    assertThat(plan("UNWIND [1] AS x MATCH (n:Big) WHERE n.k IN $ks RETURN n.k AS seq", params)).contains("[index: Big[k]]");
  }

  @Test
  void aNodeWithNoLabelIsFoundByTheIndexOfItsTypeAndTheWalkOfTheOthers() {
    // Chunk has an index on external_id, Other has none: e0 is a Chunk and an Other
    final Map<String, Object> params = Map.of("es", List.of("e3", "e0", "nope"));
    assertThat(seqs("UNWIND $es AS e MATCH (n {external_id: e}) RETURN e, coalesce(n.seq, -1) AS seq", params))
        .containsExactlyInAnyOrder(3, 0, -1);
    assertThat(seqs("UNWIND $es AS e MATCH (n) WHERE n.external_id = e RETURN e, coalesce(n.seq, -1) AS seq", params))
        .containsExactlyInAnyOrder(3, 0, -1);
  }

  // ---- labels read per row ------------------------------------------------------------------------------------------------------

  @Test
  void aLabelTheRowNamesIsMatchedPerRow() {
    final Map<String, Object> params = Map.of("labels", List.of("Chunk", "Comp", "Nothing", "Chunk"));
    final List<Integer> counts = seqs("UNWIND $labels AS l MATCH (n:$(l)) RETURN l, count(n) AS seq", params);
    // grouped by the label name: the two rows of Chunk count together, and a label no type has contributes no row
    assertThat(counts).containsExactlyInAnyOrder(2 * CHUNKS, 30);
  }

  @Test
  void aStaticLabelOnAnotherTypeOfTheSameNameIsNotANode() {
    final Map<String, Object> params = Map.of("xs", List.of(1, 2));
    assertThat(seqs("UNWIND $xs AS x MATCH (n:LINKS) RETURN x, 1 AS seq", params)).isEmpty();
  }

  // ---- helpers ---------------------------------------------------------------------------------------------------------------

  private List<Integer> seqs(final String cypher, final Map<String, Object> params) {
    final List<Integer> out = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", cypher, params)) {
      while (rs.hasNext())
        out.add(((Number) rs.next().getProperty("seq")).intValue());
    }
    return out;
  }

  private String plan(final String cypher, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + cypher, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }

  /** The candidates the first MATCH NODE step read, as PROFILE counts them (before the pattern and filter drop any). */
  private static int candidates(final String plan) {
    final Matcher matcher = MATCH_ROWS.matcher(plan);
    assertThat(matcher.find()).as(plan).isTrue();
    return Integer.parseInt(matcher.group(1).replace(",", ""));
  }

  /** The statement with an OPTIONAL MATCH in front of its RETURN, which runs it through the clause-by-clause executor. */
  private static String withOptional(final String query) {
    return query.replaceFirst(" RETURN ", OPTIONAL + " RETURN ");
  }
}
