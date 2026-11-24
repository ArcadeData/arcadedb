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
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.security.SecurityDatabaseUser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #13267: a hop from a labelled node to a node an index pins, after an UNWIND or a WITH, scanned the whole label of the first
 * node for every row and filtered on the pinned node last, where the same pattern written from the pinned end is a seek. The
 * step-based plan now starts from the pinned node when the walk back reads fewer records than the label of the first node has: the
 * seeks and every incoming relationship of the vertices they find, asked of the index when the keys are known at planning time. A
 * hub, or a key with many entries, keeps the written order. The answer is the same either way; each test asserts which node the plan
 * starts from (the first MATCH NODE step) and the rows.
 */
class CypherLabelledSourceReverseIssue13267Test {
  private static final String PATH     = "./target/databases/cypher-labelled-source-reverse-13267";
  private static final int    ENTITIES = 120;
  private static final int    EPISODES = 30;
  /** An id twice, one no entity has, a null, the first and the last entity */
  private static final List<Object> IDS = Arrays.asList("ent3", "ent3", "missing", null, "ent0", "ent119");

  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory(PATH);
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("sql", "CREATE VERTEX TYPE Entity");
    database.command("sql", "CREATE PROPERTY Entity.uuid STRING");
    database.command("sql", "CREATE INDEX ON Entity (uuid) NOTUNIQUE");
    database.command("sql", "CREATE PROPERTY Entity.grp INTEGER");
    database.command("sql", "CREATE INDEX ON Entity (grp) NOTUNIQUE");
    database.command("sql", "CREATE PROPERTY Entity.status STRING");
    database.command("sql", "CREATE INDEX ON Entity (status) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE VERTEX TYPE Doc");
    database.command("sql", "CREATE PROPERTY Doc.key STRING");
    database.command("sql", "CREATE INDEX ON Doc (key) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Episodic");
    database.command("sql", "CREATE PROPERTY Episodic.uuid STRING");
    database.command("sql", "CREATE INDEX ON Episodic (uuid) UNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Other");
    database.command("sql", "CREATE VERTEX TYPE Tiny");
    database.command("sql", "CREATE VERTEX TYPE Lonely");
    database.command("sql", "CREATE VERTEX TYPE Hashed");
    database.command("sql", "CREATE PROPERTY Hashed.key STRING");
    database.command("sql", "CREATE INDEX ON Hashed (key) NOTUNIQUE_HASH");
    database.command("sql", "CREATE VERTEX TYPE Wide");
    database.command("sql", "CREATE PROPERTY Wide.a STRING");
    database.command("sql", "CREATE PROPERTY Wide.b INTEGER");
    database.command("sql", "CREATE INDEX ON Wide (a, b) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Grouped");
    database.command("sql", "CREATE PROPERTY Grouped.grp STRING");
    database.command("sql", "CREATE INDEX ON Grouped (grp) NOTUNIQUE");
    database.command("sql", "CREATE VERTEX TYPE Big");
    database.command("sql", "CREATE EDGE TYPE MENTIONS");
    database.command("sql", "CREATE EDGE TYPE ONEWAY UNIDIRECTIONAL");
    database.transaction(() -> {
      // 120 entities, uuid unique in fact but declared NOTUNIQUE; grp has only 2 values; every tenth entity is also a Person
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Entity {uuid: 'ent' + i, grp: i % 2, name: 'n' + i})", Map.of("n", ENTITIES));
      database.command("opencypher", "MATCH (e:Entity) WHERE e.grp = 0 AND toInteger(substring(e.uuid, 3)) % 10 = 0 SET e:Person");
      database.command("opencypher", "UNWIND range(0, $n - 1) AS i CREATE (:Episodic {uuid: 'ep' + i})", Map.of("n", EPISODES));
      database.command("opencypher", "UNWIND range(0, 9) AS i CREATE (:Other {uuid: 'ot' + i})");
      database.command("opencypher", "UNWIND range(0, 99) AS i CREATE (:Doc {key: 'doc' + i})");
      database.command("opencypher", "UNWIND range(0, 2) AS i CREATE (:Tiny {uuid: 'ti' + i})");
      database.command("opencypher", "CREATE (:Lonely {uuid: 'lone'})");
      database.command("opencypher", "UNWIND range(0, 99) AS i CREATE (:Hashed {key: 'h' + i})");
      database.command("opencypher", "UNWIND range(0, 99) AS i CREATE (:Wide {a: 'a' + (i % 10), b: i})");
      database.command("opencypher", "UNWIND range(0, 299) AS i CREATE (:Grouped {grp: 'g', i: i})");
      database.command("opencypher", "UNWIND range(0, 249) AS i CREATE (:Big {i: i})");
      database.command("opencypher", "MATCH (b:Big), (x:Grouped {i: 0}) CREATE (b)-[:MENTIONS]->(x)");
      database.command("opencypher", "MATCH (t:Tiny), (h:Hashed {key: 'h5'}), (w:Wide {b: 7}) CREATE (t)-[:MENTIONS]->(h), (t)-[:MENTIONS]->(w)");
      // episode j mentions entities 3j and 3j+1, and 3j again through a second edge; every Other vertex mentions entity 0
      database.command("opencypher",
          "UNWIND range(0, $n - 1) AS j MATCH (p:Episodic {uuid: 'ep' + j}), (a:Entity {uuid: 'ent' + (3 * j)}), (b:Entity {uuid: 'ent' + (3 * j + 1)}) "
              + "CREATE (p)-[:MENTIONS]->(a), (p)-[:MENTIONS]->(b), (p)-[:MENTIONS]->(a)", Map.of("n", EPISODES));
      database.command("opencypher", "MATCH (o:Other), (a:Entity {uuid: 'ent0'}) CREATE (o)-[:MENTIONS]->(a)");
      database.command("opencypher", "MATCH (t:Tiny), (d:Doc {key: 'doc7'}) CREATE (t)-[:MENTIONS]->(d)");
      database.command("opencypher", "MATCH (t:Tiny), (e:Entity) WHERE e.grp = 1 CREATE (t)-[:MENTIONS]->(e)");
      database.command("opencypher", "MATCH (l:Lonely), (d:Doc {key: 'doc7'}) CREATE (l)-[:MENTIONS]->(d)");
      database.command("opencypher", "MATCH (e:Episodic {uuid: 'ep1'}), (a:Entity {uuid: 'ent5'}) CREATE (e)-[:ONEWAY]->(a)");
      // doc7, the hashed key h5 and the wide key (a7, 7) are mentioned by two Episodic vertices as well
      database.command("opencypher", "MATCH (e:Episodic), (d:Doc {key: 'doc7'}), (h:Hashed {key: 'h5'}), (w:Wide {b: 7}) WHERE e.uuid IN ['ep0', 'ep1'] "
          + "CREATE (e)-[:MENTIONS]->(d), (e)-[:MENTIONS]->(h), (e)-[:MENTIONS]->(w)");
      // a hub: 2,000 relationships from the Other vertices (another label than the Episodic one), and one from the first episode
      database.command("opencypher", "CREATE (:Entity {uuid: 'hub'})");
      database.command("opencypher", "MATCH (o:Other), (h:Entity {uuid: 'hub'}) UNWIND range(1, 200) AS k CREATE (o)-[:MENTIONS]->(h)");
      database.command("opencypher", "MATCH (e:Episodic {uuid: 'ep0'}), (h:Entity {uuid: 'hub'}) CREATE (e)-[:MENTIONS]->(h)");
      // a key that sorts last after 255 keys of one entry each, and holds 600 entries
      database.command("opencypher", "UNWIND range(0, 254) AS i CREATE (:Entity {uuid: 'sr' + i, status: 'a' + (1000 + i)})");
      database.command("opencypher", "UNWIND range(0, 599) AS i CREATE (:Entity {uuid: 'sz' + i, status: 'zzz'})");
      database.command("opencypher", "MATCH (t:Tiny {uuid: 'ti0'}), (e:Entity {uuid: 'sz0'}) CREATE (t)-[:MENTIONS]->(e)");
      database.command("opencypher", "MATCH (n:Entity {uuid: 'ent3'}) SET n.episode = 'ep1'");
      database.command("opencypher", "MATCH (n:Entity {uuid: 'ent4'}) SET n.episode = 'ep9'");
      database.command("opencypher", "MATCH (:Episodic {uuid: 'ep1'})-[r:MENTIONS]->(:Entity {uuid: 'ent4'}) SET r.tag = 'ep1'");
      // a second hub whose relationships are outgoing, to the Other vertices
      database.command("opencypher", "CREATE (:Entity {uuid: 'hub2'})");
      database.command("opencypher", "MATCH (o:Other), (h:Entity {uuid: 'hub2'}) UNWIND range(1, 200) AS k CREATE (h)-[:BACK]->(o)");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  /** What the pattern answers for the ids of {@link #IDS}, worked out from how the graph was built. */
  private static List<String> expectedMentions() {
    final List<String> rows = new ArrayList<>();
    for (final Object id : IDS) {
      if (id == null)
        continue;
      for (int j = 0; j < EPISODES; j++)
        for (final int entity : new int[] { 3 * j, 3 * j + 1, 3 * j })
          if (("ent" + entity).equals(id))
            rows.add(id + "|ep" + j);
    }
    return rows;
  }

  @Test
  void aNotUniqueIndexPinnedByAnUnwindRowStartsFromTheIndexedEnd() {
    final String query = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    final Map<String, Object> params = Map.of("ids", IDS);

    assertStartsFrom(query, params, "(n:Entity");
    assertThat(values(query, params)).isNotEmpty().containsExactlyInAnyOrderElementsOf(expectedMentions());
    assertThat(values(query, params)).containsExactlyInAnyOrderElementsOf(
        values("UNWIND $ids AS id MATCH (n:Entity {uuid: id})<-[r:MENTIONS]-(e:Episodic) RETURN id + '|' + e.uuid AS v", params));
  }

  @Test
  void theCountPerEntityOfGraphitisEpisodeMentionsIsTheSameFromEitherEnd() {
    final String shape = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS score, n.uuid AS uuid";
    final Map<String, Object> params = Map.of("ids", IDS);

    assertStartsFrom(shape, params, "(n:Entity");
    final Map<String, Long> counts = new HashMap<>();
    try (final ResultSet rs = database.query("opencypher", shape, params)) {
      while (rs.hasNext()) {
        final var row = rs.next();
        counts.put(row.getProperty("uuid"), ((Number) row.getProperty("score")).longValue());
      }
    }
    // ent3: edges of episode 1 twice (3j, 3j+1, 3j: 3 and 3 again), once per row of the id twice; ent0: episode 0 twice; ent119: none
    assertThat(counts).containsEntry("ent3", 4L).containsEntry("ent0", 2L).hasSize(2);
  }

  @Test
  void anEqualityInTheWhereClauseStartsFromTheIndexedEnd() {
    final String equality = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid = id RETURN id + '|' + e.uuid AS v";
    final Map<String, Object> params = Map.of("ids", IDS);

    assertStartsFrom(equality, params, "(n:Entity");
    assertThat(values(equality, params)).containsExactlyInAnyOrderElementsOf(expectedMentions());
  }

  @Test
  void aUniqueKeyStartsFromTheTargetToo() {
    // doc7 has 6 incoming relationships (2 Episodic, 3 Tiny, 1 Lonely): the walk back reads about 8 records, the 30 Episodic vertices more
    final String query = "UNWIND $keys AS k MATCH (e:Episodic)-[r:MENTIONS]->(d:Doc {key: k}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of("keys", List.of("doc7", "nokey")), "(d:Doc");
    assertThat(longs(query, Map.of("keys", List.of("doc7", "nokey")))).containsExactly(2L);
    // the 3 Tiny vertices are fewer than that
    final String tiny = "UNWIND $keys AS k MATCH (t:Tiny)-[r:MENTIONS]->(d:Doc {key: k}) RETURN count(*) AS c";
    assertStartsFrom(tiny, Map.of("keys", List.of("doc7", "nokey")), "(t:Tiny");
    assertThat(longs(tiny, Map.of("keys", List.of("doc7", "nokey")))).containsExactly(3L);
  }

  @Test
  void anInListStartsFromTheTargetOnlyWhenItsKeysAreFewerThanTheLabel() {
    // each key is one seek: 20 keys no vertex has read 20 records, 40 read more than the 30 Episodic vertices
    final String few = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(d:Doc) WHERE d.key IN " + keys(20) + " RETURN count(*) AS c";
    final String many = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(d:Doc) WHERE d.key IN " + keys(40) + " RETURN count(*) AS c";
    assertStartsFrom(few, Map.of(), "(d:Doc");
    assertStartsFrom(many, Map.of(), "(e:Episodic");
    assertThat(longs(few, Map.of())).containsExactly(0L);
    assertThat(longs(many, Map.of())).containsExactly(0L);

    // a list that is a parameter is as long as the parameter is: planned from it, not from a guess
    final String parameter = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(d:Doc) WHERE d.key IN $keys RETURN count(*) AS c";
    assertStartsFrom(parameter, Map.of("keys", List.of("doc7")), "(d:Doc");
    assertThat(longs(parameter, Map.of("keys", List.of("doc7")))).containsExactly(2L);
    final List<String> million = new ArrayList<>(List.of("doc7"));
    for (int i = 0; i < 5000; i++)
      million.add("absent" + i);
    assertStartsFrom(parameter, Map.of("keys", million), "(e:Episodic");
    assertThat(longs(parameter, Map.of("keys", million))).containsExactly(2L);

    // the entities: 2 keys of a NOTUNIQUE index that holds one entry per key, one with 3 incoming relationships and one with 1
    final String entities = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid IN ['ent3', 'ent4'] RETURN n.uuid + '|' + e.uuid AS v";
    assertStartsFrom(entities, Map.of(), "(n:Entity");
    assertThat(values(entities, Map.of())).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent4|ep1");
    final String list = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid IN $ids RETURN n.uuid + '|' + e.uuid AS v";
    assertStartsFrom(list, Map.of("ids", List.of("ent3", "missing")), "(n:Entity");
    assertThat(values(list, Map.of("ids", List.of("ent3", "missing")))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1");
  }

  @Test
  void aHashIndexIsAskedForItsKeysAndNeverWalkedInOrder() {
    // the keys are known: the index is asked for them, whatever its kind
    final String known = "UNWIND ['h5'] AS k MATCH (e:Episodic)-[r:MENTIONS]->(h:Hashed {key: k}) RETURN count(*) AS c";
    assertStartsFrom(known, Map.of(), "(h:Hashed");
    assertThat(longs(known, Map.of())).containsExactly(2L);
    // a key only a row knows needs an estimate from the order of the index, which a hash index has not: written order, no failure
    final String unknown = "WITH 'h5' AS k MATCH (e:Episodic)-[r:MENTIONS]->(h:Hashed {key: k}) RETURN count(*) AS c";
    assertStartsFrom(unknown, Map.of(), "(e:Episodic");
    assertThat(longs(unknown, Map.of())).containsExactly(2L);
  }

  @Test
  void aCompositeKeyIsSeekedByEqualitiesNotByLists() {
    final String lists = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(w:Wide) WHERE w.a IN ['a7', 'a8'] AND w.b IN [7, 8] RETURN count(*) AS c";
    assertStartsFrom(lists, Map.of(), "(e:Episodic");
    assertThat(longs(lists, Map.of())).containsExactly(2L);
    // 250 Big vertices against the estimate of the walk back (one entry, and the average in-degree of the type)
    final String whole = "UNWIND [7] AS b MATCH (g:Big)-[r:MENTIONS]->(w:Wide {a: 'a7', b: b}) RETURN count(*) AS c";
    assertStartsFrom(whole, Map.of(), "(w:Wide");
    assertThat(longs(whole, Map.of())).containsExactly(0L);
  }

  @Test
  void aKeyWithManyEntriesKeepsTheWrittenOrder() {
    // 300 Grouped vertices share the key: the seek alone reads more than the 250 Big vertices, whatever their in-degrees
    final String query = "UNWIND ['g'] AS g MATCH (b:Big)-[r:MENTIONS]->(x:Grouped {grp: g}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(b:Big");
    assertThat(longs(query, Map.of())).containsExactly(250L);
  }

  @Test
  void aKeyThatSortsLastAfterManyRareOnesIsCountedNotSampled() {
    // 255 keys of one entry sort first and 'zzz' (600 entries) last: a sample of the first entries would read it as 1 entry per key
    final String known = "UNWIND ['zzz'] AS s MATCH (t:Tiny)-[r:MENTIONS]->(n:Entity {status: s}) RETURN count(*) AS c";
    assertStartsFrom(known, Map.of(), "(t:Tiny");
    assertThat(longs(known, Map.of())).containsExactly(1L);
    final String literal = "WITH 1 AS one MATCH (t:Tiny)-[r:MENTIONS]->(n:Entity) WHERE n.status = 'zzz' RETURN count(*) AS c";
    assertStartsFrom(literal, Map.of(), "(t:Tiny");
    assertThat(longs(literal, Map.of())).containsExactly(1L);
    // a key only a row knows is estimated from both ends of the index: the end the key fills says nothing about its size
    final String unknown = "WITH 'zzz' AS s MATCH (t:Tiny)-[r:MENTIONS]->(n:Entity {status: s}) RETURN count(*) AS c";
    assertStartsFrom(unknown, Map.of(), "(t:Tiny");
    assertThat(longs(unknown, Map.of())).containsExactly(1L);
    // a rare key of the same index is the seek the plan starts from
    final String rare = "UNWIND ['a1000'] AS s MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {status: s}) RETURN count(*) AS c";
    assertStartsFrom(rare, Map.of(), "(n:Entity");
    assertThat(longs(rare, Map.of())).containsExactly(0L);
  }

  @Test
  void aHubTargetKeepsTheWrittenOrder() {
    // the hub has 2,000 incoming relationships from vertices of another label and one from an Episodic: the 30 Episodic vertices are the cheaper side
    final String literal = "UNWIND range(1, 5) AS i MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: 'hub'}) RETURN count(*) AS c";
    assertStartsFrom(literal, Map.of(), "(e:Episodic");
    assertThat(longs(literal, Map.of())).containsExactly(5L);
    final String fromList = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertStartsFrom(fromList, Map.of("ids", List.of("hub", "hub", "ent3")), "(e:Episodic");
    assertThat(longs(fromList, Map.of("ids", List.of("hub", "hub", "ent3")))).containsExactly(4L);
    final String where = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid IN $ids RETURN count(*) AS c";
    assertStartsFrom(where, Map.of("ids", List.of("hub")), "(e:Episodic");
    assertThat(longs(where, Map.of("ids", List.of("hub")))).containsExactly(1L);
    // an entity next to the hub keeps the seek the Graphiti statement is planned with
    assertStartsFrom(fromList, Map.of("ids", List.of("ent3", "ent4")), "(n:Entity");
  }

  @Test
  void aTargetMapThatReadsTheFirstNodeKeepsTheWrittenOrderAndItsRows() {
    // ent3 says it was mentioned by ep1, ent4 by ep9: only the first pair is a row, twice (two relationships), once per listed id
    final List<Object> ids = Arrays.asList("ent3", "ent3", null, "ent4", "ent119");
    final String query = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id, episode: e.uuid}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(query, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(query, Map.of("ids", ids))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent3|ep1", "ent3|ep1");
    // a value no vertex has is null, which equals nothing
    final String missing = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id, episode: e.missing}) RETURN id AS v";
    assertStartsFrom(missing, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(missing, Map.of("ids", ids))).isEmpty();
  }

  @Test
  void aRelationshipPredicateThatReadsTheFirstNodeKeepsTheWrittenOrderAndItsRows() {
    final List<Object> ids = Arrays.asList("ent3", "ent3", null, "ent4", "ent0");
    final String where = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS WHERE e.uuid = 'ep1']->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(where, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(where, Map.of("ids", ids))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent3|ep1", "ent3|ep1", "ent4|ep1");
    final String map = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS {tag: e.uuid}]->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(map, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(map, Map.of("ids", ids))).containsExactlyInAnyOrder("ent4|ep1");
    // a predicate that reads only the relationship is evaluated whichever end the plan starts from
    final String own = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS WHERE r.tag = 'ep1']->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(own, Map.of("ids", ids), "(n:Entity");
    assertThat(values(own, Map.of("ids", ids))).containsExactlyInAnyOrder("ent4|ep1");
  }

  @Test
  void aRelationshipPredicateThatReadsTheTargetKeepsTheWrittenOrderWhateverItAnswers() {
    // the written order evaluates a relationship predicate before the target is bound, so n.uuid reads null there and no row passes;
    // a plan that starts from the target would bind it first and answer rows. Which of the two is right was not checked against Neo4j;
    // the safe choice is the answer of main, which never reversed this hop: the written order, whatever it answers
    final String query = "UNWIND ['ent3'] AS id MATCH (e:Episodic)-[r:MENTIONS WHERE n.uuid = 'ent3']->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(e:Episodic");
    final String blocked = "UNWIND ['ent3'] AS id MATCH (e:Episodic)-[r:MENTIONS WHERE n.uuid = 'ent3']->(n:Entity {uuid: id}) WHERE e.uuid = e.uuid "
        + "RETURN count(*) AS c";
    assertThat(longs(query, Map.of())).isEqualTo(longs(blocked, Map.of()));
  }

  @Test
  void aComprehensionAndAnExistsSubqueryThatReadTheFirstNodeKeepTheWrittenOrderAndItsRows() {
    final List<Object> ids = Arrays.asList("ent3", "ent3", null, "ent4", "ent119");
    final String list = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id, episode: head([x IN [e.uuid] | x])}) "
        + "RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(list, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(list, Map.of("ids", ids))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent3|ep1", "ent3|ep1");
    final String pattern = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id, "
        + "episode: head([(e)-[:MENTIONS]->(m:Entity {uuid: 'ent3'}) | e.uuid])}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(pattern, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(pattern, Map.of("ids", ids))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent3|ep1", "ent3|ep1");
    final String exists = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS WHERE EXISTS { MATCH (e)-[:MENTIONS]->(:Entity {uuid: 'ent4'}) }]->"
        + "(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(exists, Map.of("ids", ids), "(e:Episodic");
    assertThat(values(exists, Map.of("ids", ids))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent3|ep1", "ent3|ep1", "ent4|ep1");
  }

  @Test
  void aCommaPatternAndAnUnwindAfterAWithKeepTheRows() {
    // the second pattern ends on the vertex the first one found: 2 relationships from ep0 times the 10 Other vertices that mention ent0
    final String comma = "UNWIND ['ent0'] AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}), (o:Other)-[:MENTIONS]->(n) RETURN count(*) AS c";
    assertThat(longs(comma, Map.of())).containsExactly(20L);
    final String commaBlocked = "UNWIND ['ent0'] AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}), (o:Other)-[:MENTIONS]->(n) "
        + "WHERE e.uuid = e.uuid RETURN count(*) AS c";
    assertThat(longs(commaBlocked, Map.of())).containsExactly(20L);

    final String afterWith = "WITH 1 AS one UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(afterWith, Map.of("ids", List.of("ent3", "ent4")), "(n:Entity");
    assertThat(values(afterWith, Map.of("ids", List.of("ent3", "ent4")))).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent4|ep1");
  }

  @Test
  void anOrOfTwoKeysIsAScanOfTheTypeNotTwoSeeks() {
    // the node step seeks the conjuncts of the WHERE clause, not a disjunction: it would scan the 976 Entity vertices
    final String query = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid = 'ent3' OR n.uuid = 'ent4' RETURN n.uuid + '|' + e.uuid AS v";
    assertStartsFrom(query, Map.of(), "(e:Episodic");
    assertThat(values(query, Map.of())).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent4|ep1");
  }

  @Test
  void aListInsideTheKeyListIsNotCountedAsKeys() {
    final String query = "WITH 1 AS one MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity) WHERE n.uuid IN [['x'], 'ent3'] RETURN n.uuid + '|' + e.uuid AS v";
    assertStartsFrom(query, Map.of(), "(e:Episodic");
    assertThat(values(query, Map.of())).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1");
  }

  @Test
  void aNameAWithGivesAnotherValueIsNotPricedAsTheUnwoundList() {
    // id is the hub in the UNWIND and ent3 after the WITH: the plan weighs ent3, which the 30 Episodic vertices outnumber
    final String query = "UNWIND ['hub', 'hub'] AS id WITH 'ent3' AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN id + '|' + e.uuid AS v";
    assertStartsFrom(query, Map.of(), "(n:Entity");
    assertThat(values(query, Map.of())).containsExactlyInAnyOrder("ent3|ep1", "ent3|ep1", "ent3|ep1", "ent3|ep1");
  }

  @Test
  void theKeysAreWeightedByTheRowsThatHoldThem() {
    // 100 rows read ent3 (about 7 records each) and one reads the hub (counted at the ceiling): the mean over the rows is under the 30
    // Episodic vertices, the mean over the two distinct keys is not
    final List<String> ids = new ArrayList<>();
    for (int i = 0; i < 100; i++)
      ids.add("ent3");
    ids.add("hub");
    final String query = "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of("ids", ids), "(n:Entity");
    assertThat(longs(query, Map.of("ids", ids))).containsExactly(201L);
  }

  @Test
  void aHopWrittenTowardsTheFirstNodeIsWeighedByTheEdgesTheWalkFollows() {
    // the hub has 2,000 outgoing BACK relationships and none incoming: written (o)<-[:BACK]-(hub) the walk from the hub follows them
    final String query = "UNWIND range(1, 5) AS i MATCH (o:Other)<-[r:BACK]-(n:Entity {uuid: 'hub2'}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(o:Other");
    assertThat(longs(query, Map.of())).containsExactly(5L * 2000L);
  }

  private static String keys(final int count) {
    final StringBuilder list = new StringBuilder("[");
    for (int i = 0; i < count; i++)
      list.append(i == 0 ? "'x" : ", 'x").append(i).append("'");
    return list.append("]").toString();
  }

  @Test
  void aLabelWithoutVerticesOrOneThatIsNotTheEndOfTheEdgesKeepsTheRowsRight() {
    // Other vertices mention entity 0 too: the label of the first node still filters what the walk back from the entity finds
    final String other = "UNWIND ['ent0'] AS id MATCH (o:Other)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(other, Map.of())).containsExactly(10L);
    final String episodic = "UNWIND ['ent0'] AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(episodic, Map.of())).containsExactly(2L);
    final String person = "UNWIND ['ent0', 'ent1', 'ent10'] AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Person {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(person, Map.of())).containsExactly(3L);
    final String absent = "UNWIND ['ent0'] AS id MATCH (g:Ghost)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(absent, Map.of())).containsExactly(0L);
    // both labels of the first node
    final String both = "UNWIND ['ent0'] AS id MATCH (e:Episodic:Other)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(longs(both, Map.of())).containsExactly(0L);
  }

  @Test
  void aKeyThatManyVerticesShareKeepsTheWrittenOrder() {
    // grp has 2 values over 120 entities, 60 each: the seek finds more than the 3 Tiny vertices have
    final String query = "UNWIND [0, 1] AS g MATCH (t:Tiny)-[r:MENTIONS]->(n:Entity {grp: g}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(t:Tiny");
    assertThat(longs(query, Map.of())).containsExactly(180L);
  }

  @Test
  void aSingleVertexLabelKeepsTheWrittenOrder() {
    final String query = "UNWIND ['doc7'] AS k MATCH (l:Lonely)-[r:MENTIONS]->(d:Doc {key: k}) RETURN count(*) AS c";
    assertStartsFrom(query, Map.of(), "(l:Lonely");
    assertThat(longs(query, Map.of())).containsExactly(1L);
  }

  @Test
  void anythingElseKeepsTheWrittenOrder() {
    final List<String> queries = List.of(
        // a first node a property pins, or an equality of the WHERE clause
        "UNWIND $ids AS id MATCH (e:Episodic {uuid: 'ep1'})-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) WHERE e.uuid = 'ep1' RETURN count(*) AS c",
        // an edge type declared unidirectional, an untyped hop, a path variable, a variable-length hop
        "UNWIND $ids AS id MATCH (e:Episodic)-[r:ONEWAY]->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH (e:Episodic)-->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH p = (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c",
        "UNWIND $ids AS id MATCH (e:Episodic)-[r:MENTIONS*1..2]->(n:Entity {uuid: id}) RETURN count(*) AS c");
    for (final String query : queries) {
      final String first = lineOf(profile(query, Map.of("ids", IDS)), "MATCH NODE");
      assertThat(first).as(query).doesNotContain("(n:Entity").doesNotContain("(n)");
    }
    assertThat(lineOf(profile("UNWIND $ids AS id MATCH (n:Entity {uuid: id})<-[r:MENTIONS]-(e:Episodic) RETURN count(*) AS c", Map.of("ids", IDS)),
        "MATCH NODE")).contains("(n:Entity");

    // an OPTIONAL MATCH binds its pattern in the order written
    final String optional = "UNWIND $ids AS id OPTIONAL MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: id}) RETURN count(*) AS c";
    assertThat(lineOf(profile(optional, Map.of("ids", IDS)), "MATCH NODE")).contains("(e:Episodic");
  }

  /** A user denied a vertex type reads the same rows from either end: the type's vertices are left out or the statement is refused. */
  @Nested
  class RestrictedUser {
    private static final String ACL_PATH = "./target/databases/cypher-labelled-source-reverse-13267-acl";

    private DatabaseFactory factory;
    private Database        aclDatabase;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(ACL_PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      aclDatabase = factory.create();
      aclDatabase.command("sql", "CREATE VERTEX TYPE Entity");
      aclDatabase.command("sql", "CREATE PROPERTY Entity.uuid STRING");
      aclDatabase.command("sql", "CREATE INDEX ON Entity (uuid) NOTUNIQUE");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Episodic");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Secret");
      aclDatabase.command("sql", "CREATE EDGE TYPE MENTIONS");
      aclDatabase.command("sql", "CREATE VERTEX TYPE Hidden EXTENDS Entity");
      aclDatabase.transaction(() -> {
        aclDatabase.command("opencypher", "UNWIND range(1, 5) AS i CREATE (:Entity {uuid: 'e' + i})");
        aclDatabase.command("opencypher", "MATCH (n:Entity {uuid: 'e1'}) UNWIND range(1, 5) AS i "
            + "CREATE (:Episodic {i: i})-[:MENTIONS]->(n), (:Secret {i: 100 + i})-[:MENTIONS]->(n)");
      });
    }

    @AfterEach
    void tearDown() {
      DatabaseContext.INSTANCE.getContext(aclDatabase.getDatabasePath()).setCurrentUser(null);
      aclDatabase.drop();
      factory.close();
    }

    @Test
    void aDeniedLabelOfTheFirstNodeIsNeverReadWhicheverEndTheStatementStartsFrom() {
      final String written = "UNWIND ['e1'] AS id MATCH (s:Secret)-[:MENTIONS]->(n:Entity {uuid: id}) RETURN s.i AS i";
      final String reversed = "UNWIND ['e1'] AS id MATCH (n:Entity {uuid: id})<-[:MENTIONS]-(s:Secret) RETURN s.i AS i";
      assertThat(outcomeOf(written)).isEqualTo("[101, 102, 103, 104, 105]");

      bindUserDenying("Secret");
      final String outcome = outcomeOf(written);
      assertThat(outcome).as("no vertex of the denied type is read").isIn("[]", "SecurityException");
      assertThat(outcomeOf(reversed)).isEqualTo(outcome);
    }

    @Test
    void aDeniedSubtypeOfTheTargetDoesNotFailThePlanning() {
      // the counts the plan weighs are read at planning time: a user denied a subtype of the target must get what the written order
      // answers, which never reads that subtype, and not the refusal of a count it asked for itself
      final String query = "UNWIND ['e1'] AS id MATCH (s:Episodic)-[:MENTIONS]->(n:Entity {uuid: id}) RETURN s.i AS i";
      assertThat(outcomeOf(query)).isEqualTo("[1, 2, 3, 4, 5]");

      bindUserDenying("Hidden");
      assertThat(outcomeOf(query)).isEqualTo("[1, 2, 3, 4, 5]");
    }

    private void bindUserDenying(final String typeName) {
      final Set<Integer> deniedBuckets = new HashSet<>(aclDatabase.getSchema().getType(typeName).getBucketIds(false));
      DatabaseContext.INSTANCE.getContext(aclDatabase.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "restricted";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return !deniedBuckets.contains(fileId);
        }

        @Override
        public boolean requestAccessOnType(final String name, final ACCESS access) {
          return !typeName.equals(name);
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }

    /** The sorted rows of the statement, or the kind of the exception that ended it. */
    private String outcomeOf(final String query) {
      try {
        final List<Integer> values = new ArrayList<>();
        try (final ResultSet rs = aclDatabase.query("opencypher", query)) {
          while (rs.hasNext())
            values.add(((Number) rs.next().getProperty("i")).intValue());
        }
        values.sort(null);
        return String.valueOf(values);
      } catch (final RuntimeException e) {
        return e.getClass().getSimpleName();
      }
    }
  }

  private void assertStartsFrom(final String query, final Map<String, Object> params, final String firstNode) {
    final String plan = profile(query, params);
    assertThat(plan).as("the test is about the step-based plan").doesNotContain("Cost-Based");
    assertThat(lineOf(plan, "MATCH NODE")).as(plan).contains(firstNode);
    assertThat(plan.indexOf(firstNode)).as(plan).isLessThan(plan.indexOf("MATCH RELATIONSHIP"));
  }

  private static String lineOf(final String plan, final String text) {
    for (final String line : plan.split("\n"))
      if (line.contains(text))
        return line;
    return "";
  }

  private List<String> values(final String query, final Map<String, Object> params) {
    final List<String> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(String.valueOf(rs.next().<Object>getProperty("v")));
    }
    return values;
  }

  private List<Long> longs(final String query, final Map<String, Object> params) {
    final List<Long> values = new ArrayList<>();
    try (final ResultSet rs = database.query("opencypher", query, params)) {
      while (rs.hasNext())
        values.add(((Number) rs.next().getProperty("c")).longValue());
    }
    return values;
  }

  private String profile(final String query, final Map<String, Object> params) {
    try (final ResultSet rs = database.query("opencypher", "PROFILE " + query, params)) {
      while (rs.hasNext())
        rs.next();
      return rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2);
    }
  }
}
