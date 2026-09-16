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
import com.arcadedb.database.Document;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.InvalidPropertyTypeException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests for issue #7729, the follow-up to #7629.
 * <p>
 * #7629 settled that a map-valued property is refused by every openCypher write clause, matching Neo4j's "Property
 * values can only be of primitive types or arrays thereof" ({@code Neo.ClientError.Statement.TypeError}). SQL is
 * deliberately not bound by that rule - {@code MAP} is a first-class ArcadeDB schema type - so a record can carry a
 * map property that openCypher can read but can never write. That leaves two gaps this class closes.
 * <p>
 * First, the SQL-written / openCypher-read path had no test at all, so nothing stopped a future change from hiding
 * or stringifying such a property. Reads are and must stay permissive: in a multi-model database openCypher is one
 * lens on a store shared with SQL, and silently dropping what another query language wrote would be worse than
 * returning a value Neo4j itself would never have produced.
 * <p>
 * Second, the refusal message was written for the literal case ({@code SET n.x = {y: 1}}), where the offending value
 * is visible in the query text. On the copy paths - {@code SET t = n} and {@code SET t.m2 = n.m} against a record
 * whose map was written by SQL - the caller wrote no map at all, and the message named neither the property being
 * written nor where the value came from. Worse, it was an {@link IllegalArgumentException}, which Bolt could only
 * classify as {@code Neo.DatabaseError.General.UnknownError}, and whose message the {@code CommandExecutionException}
 * wrapper replaced with the query text - so the reporter's client saw an unexplained server fault. It is now an
 * {@link InvalidPropertyTypeException}, which passes the wrapper unchanged and carries its own diagnosis.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@SuppressWarnings("unchecked")
class CypherMapPropertyDiagnosticsIssue7729Test {
  private Database database;

  @BeforeEach
  void setUp() {
    database = new DatabaseFactory("./target/databases/testcypher-7729").create();
    database.getSchema().createVertexType("R");
    database.getSchema().createVertexType("T");
    database.getSchema().createEdgeType("REL");
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX R SET id = 1, m = {'k': 1, 'nested': {'deep': true}}, plain = 'x'");
      database.command("sql", "CREATE VERTEX T SET id = 2");
    });
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      database = null;
    }
  }

  private Result single(final String cypher) {
    try (final ResultSet rs = database.query("opencypher", cypher)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next();
    }
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Reads: permissive, and must stay that way.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aSqlWrittenMapPropertyIsReturnedWholeByCypher() {
    final Object value = single("MATCH (n:R) RETURN n.m AS m").getProperty("m");
    assertThat(value).isInstanceOf(Map.class);
    assertThat((Map<String, Object>) value).containsEntry("k", 1).containsEntry("nested", Map.of("deep", true));
  }

  @Test
  void aSqlWrittenMapPropertyRidesAlongInsideTheNode() {
    final Result row = single("MATCH (n:R) RETURN n");
    final Object node = row.getProperty("n");
    assertThat(node).isInstanceOf(Document.class);
    assertThat(((Document) node).get("m")).isInstanceOf(Map.class);
  }

  @Test
  void aSqlWrittenMapPropertyIsNavigableWithDotAndBracketSyntax() {
    assertThat(single("MATCH (n:R) RETURN n.m.k AS k").<Object>getProperty("k")).isEqualTo(1);
    assertThat(single("MATCH (n:R) RETURN n.m['k'] AS k").<Object>getProperty("k")).isEqualTo(1);
    assertThat(single("MATCH (n:R) RETURN n.m.nested.deep AS d").<Object>getProperty("d")).isEqualTo(true);
  }

  @Test
  void aSqlWrittenMapPropertyIsUsableInAPredicate() {
    assertThat(single("MATCH (n:R) WHERE n.m.k = 1 RETURN n.id AS id").<Object>getProperty("id")).isEqualTo(1);
  }

  @Test
  void aSqlWrittenMapPropertyIsVisibleToPropertiesAndKeys() {
    final Object properties = single("MATCH (n:R) RETURN properties(n) AS p").getProperty("p");
    assertThat((Map<String, Object>) properties).containsKey("m");
    assertThat(single("MATCH (n:R) RETURN keys(n) AS k").<List<Object>>getProperty("k")).contains("m");
  }

  @Test
  void writingASiblingPropertyLeavesTheMapAloneAndDoesNotRevalidateIt() {
    database.transaction(() -> database.command("opencypher", "MATCH (n:R) SET n.other = 1"));

    assertThat(single("MATCH (n:R) RETURN n.other AS o").<Object>getProperty("o")).isEqualTo(1L);
    assertThat(single("MATCH (n:R) RETURN n.m AS m").<Object>getProperty("m")).isInstanceOf(Map.class);
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Writes: refused, and the refusal has to say why.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void copyingAWholeRecordThatCarriesAMapNamesThePropertyAndTheSourceRecord() {
    final String sourceRid = single("MATCH (n:R) RETURN n.id AS id, elementId(n) AS rid").getProperty("rid").toString();

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t = n")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("TypeError: InvalidPropertyType")
        .hasMessageContaining("property 'm'")
        .hasMessageContaining(sourceRid)
        .hasMessageContaining("SQL");
  }

  @Test
  void copyingASingleMapPropertyNamesTheTargetPropertyAndTheExpressionItCameFrom() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t.m2 = n.m")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm2'")
        .hasMessageContaining("n.m");
  }

  @Test
  void mergingAWholeRecordThatCarriesAMapNamesTheProperty() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t += n")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm'");
  }

  @Test
  void aNewlineInAnIdentifierCannotForgeALogLine() {
    // A property name is caller-supplied text - Cypher's backtick-quoted identifiers accept a newline inside one -
    // and this message is written to the server log as a line (CWE-117).
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t.`a\nFAKE LOG LINE` = {k: 1}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("\n"))
        .hasMessageContaining("FAKE LOG LINE");
  }

  @Test
  void aNewlineInAParameterNameCannotForgeALogLineEither() {
    // The parameter clause is the third place a caller-supplied name reaches the message, and it used to be the one
    // that neither cleaned nor bounded it. Cypher's backtick quoting applies to a parameter name as much as to a
    // property name.
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 9, m: $`a\nFAKE LOG LINE`})", Map.of("a\nFAKE LOG LINE", Map.of("k", 1)))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("\n"))
        .hasMessageContaining("FAKE LOG LINE");
  }

  @Test
  void aVeryLongPropertyNameIsCutShortLikeEveryOtherNameTheMessageEchoes() {
    final String longName = "p".repeat(200);

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.`" + longName + "` = {k: 1}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain(longName));
  }

  @Test
  void cuttingALongNameShortNeverSplitsACharacterInHalf() {
    // An astral character is two chars in UTF-16. Cutting between its halves leaves a lone surrogate, which renders
    // as a replacement character wherever the message is read. 39 letters puts one straddling the cut.
    final String name = "p".repeat(39) + "\uD83D\uDE00".repeat(5);

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t.`" + name + "` = {k: 1}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .satisfies(e -> {
          for (int i = 0; i < e.getMessage().length(); i++)
            assertThat(Character.isSurrogate(e.getMessage().charAt(i))
                && !(Character.isHighSurrogate(e.getMessage().charAt(i)) && i + 1 < e.getMessage().length()
                    && Character.isLowSurrogate(e.getMessage().charAt(i + 1)))
                && !(Character.isLowSurrogate(e.getMessage().charAt(i)) && i > 0
                    && Character.isHighSurrogate(e.getMessage().charAt(i - 1))))
                .as("lone surrogate at %d in: %s", i, e.getMessage()).isFalse();
        });
  }

  @Test
  void aUnicodeLineSeparatorIsTreatedAsALineBreakToo() {
    // U+2028 is not an ISO control character, but plenty of log viewers and parsers break a line on it.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t.`a\u2028FAKE LOG LINE` = {k: 1}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("\u2028"))
        .hasMessageContaining("FAKE LOG LINE");
  }

  @Test
  void anExpressionTooLongToNameIsSimplyNotNamed() {
    // The third bound in the message, and the only one without a test of its own: an expression's text is echoed
    // only up to MAX_ECHOED_EXPRESSION_LENGTH. Past it the clause is dropped rather than truncated, because half an
    // expression names nothing.
    final String longParameter = "p".repeat(150);

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.m = $" + longParameter, Map.of(longParameter, Map.of("k", 1)))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm'")
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain(longParameter))
        .hasMessageNotContaining("produced by the expression");
  }

  @Test
  void aBareParameterReadsTheSameWhicheverClauseRefusedIt() {
    // CREATE and MERGE resolve a bare parameter to its name before validating; SET passes the expression. Both
    // describe the same thing, so both say it the same way.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t.m = $p", Map.of("p", Map.of("k", 1)))))
        .hasMessageContaining("supplied by parameter $p");

    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:R {id: 9, m: $p})", Map.of("p", Map.of("k", 1)))))
        .hasMessageContaining("supplied by parameter $p");
  }

  @Test
  void aParameterSourcedMapNamesTheEntryAndTheParameterItCameFrom() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t += $p", Map.of("p", Map.of("payload", Map.of("k", 1))))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'payload'")
        .hasMessageContaining("$p");
  }

  @Test
  void createNamesThePropertyItRefused() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "CREATE (n:R {id: 9, m: $m})", Map.of("m", Map.of("k", 1)))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm'");
  }

  @Test
  void mergeCreationBranchNamesThePropertyAndTheParameterItRefused() {
    // MERGE used to name only the property, because evaluateProperties() hands back the evaluated values alone.
    // The pattern's own property map is still in scope at the write, so the origin is read back out of it there.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MERGE (n:R {id: 9, m: $m})", Map.of("m", Map.of("k", 1)))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm'")
        .hasMessageContaining("$m");
  }

  @Test
  void mergeEdgeCreationBranchAlsoNamesTheParameter() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (n:R), (t:T) MERGE (n)-[r:REL {m: $m}]->(t)", Map.of("m", Map.of("k", 1)))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm'")
        .hasMessageContaining("$m");
  }

  @Test
  void aListCarryingAMapNamesThePropertyAndSaysTheMapWasInsideTheList() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t.tags = [1, {k: 2}]")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'tags'")
        .hasMessageContaining("list");
  }

  @Test
  void aWideMapNamesTheFirstFewKeysAndCountsTheRest() {
    // The one piece of message-building that reads a magic constant: MAX_DESCRIBED_KEYS caps the enumeration so a
    // wide map cannot turn one refusal into a page of client response and server log. Six keys, so exactly one is
    // left to count. The keys named are whichever the map iterates first, which is why only the count is asserted
    // exactly - the cap, not the ordering, is the behaviour being pinned.
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.wide = {a: 1, b: 2, c: 3, d: 4, e: 5, f: {nested: 1}}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'wide'")
        .hasMessageContaining("... 1 more");
  }

  @Test
  void aMapNarrowerThanTheCapNamesEveryKeyAndCountsNothing() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.narrow = {a: 1, b: 2, c: 3, d: 4, e: 5}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("[a, b, c, d, e]")
        .hasMessageNotContaining("more");
  }

  @Test
  void aLiteralRightHandSideIsNeverEchoedBackSoItsValuesStayOutOfTheMessageAndTheLog() {
    // The message reports a refused map by its keys and never its contents, because it reaches both a client and
    // the server log. An expression's text is only named when it NAMES the value (n.m, $p); a map literal's text
    // IS its values, so echoing it would put back exactly what the key-only rule keeps out.
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.creds = {password: 'hunter2', token: 'abc'}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'creds'")
        .hasMessageContaining("password")   // the key identifies what was refused
        .hasMessageNotContaining("hunter2") // the value never leaves the query
        .hasMessageNotContaining("abc");
  }

  @Test
  void createDoesNotEchoALiteralRightHandSideEither() {
    // The SET counterpart is aLiteralRightHandSideIsNeverEchoedBackSoItsValuesStayOutOfTheMessageAndTheLog. CREATE
    // reaches the same place by a different road - a literal map arrives already evaluated, so there is no origin
    // to echo rather than one that is refused - and the two must agree on what the caller is told.
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "CREATE (n:R {id: 9, creds: {password: 'hunter2'}})")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'creds'")
        .hasMessageContaining("password")
        .hasMessageNotContaining("hunter2");
  }

  @Test
  void aValueCopiedFromAnotherPropertyIsStillNamedByTheExpressionThatReadIt() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t.m2 = n.m")))
        .hasMessageContaining("produced by the expression n.m");
  }

  @Test
  void anEmptyMapIsStillRefusedAndSaysItHadNoEntries() {
    // Not point-shaped, so it takes the ordinary map branch - and the branch of describeKeys that has no keys to
    // name at all.
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "MATCH (t:T) SET t.m = {}")))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("property 'm'")
        .hasMessageContaining("with no entries");
  }

  @Test
  void oneVeryLongKeyIsCutShortSoItCannotCarryItsOwnPayloadIntoTheLog() {
    // Capping how MANY keys are named is only half the bound: a key is caller-supplied text too.
    final String longKey = "k".repeat(200);

    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.m = $p", Map.of("p", Map.of(longKey, Map.of("x", 1))))))
        .isInstanceOf(InvalidPropertyTypeException.class)
        .hasMessageContaining("...")
        .hasMessageNotContaining(longKey);
  }

  @Test
  void theRefusalNamesTheOffendingKeysSoTheValueCanBeIdentified() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t.m2 = n.m")))
        .hasMessageContaining("k")
        .hasMessageContaining("nested");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // The refusal has to survive the trip to a client: right category, right message, no generic server fault.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void theRefusalIsAClientValidationErrorAndNotAServerFault() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t = n")))
        .satisfies(e -> assertThat(ErrorCategory.of(e)).isEqualTo(ErrorCategory.VALIDATION));
  }

  @Test
  void theDiagnosisReachesTheOutermostMessageInsteadOfBeingReplacedByTheQueryText() {
    // The Bolt/HTTP layers report the outermost throwable's message. Before #7729 the openCypher engine wrapped the
    // IllegalArgumentException in a CommandExecutionException whose message is just the query text, so the caller
    // was told only that "MATCH ... SET t = n" failed. Subclassing CommandExecutionException makes the engine
    // rethrow it untouched, so the diagnosis is what the client reads.
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "MATCH (n:R), (t:T) SET t = n")))
        .hasMessageContaining("TypeError: InvalidPropertyType")
        .hasMessageContaining("property 'm'");
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Nothing above may loosen the rule itself.
  // ---------------------------------------------------------------------------------------------------------------

  @Test
  void aPointValuedPropertyStaysExemptAndIsStillWritable() {
    database.transaction(
        () -> database.command("opencypher", "MATCH (t:T) SET t.loc = point({x: 1.0, y: 2.0, crs: 'cartesian'})"));

    final Object loc = single("MATCH (t:T) RETURN t.loc AS loc").getProperty("loc");
    assertThat(loc).isInstanceOf(Map.class);
    assertThat((Map<String, Object>) loc).containsEntry("x", 1.0).containsEntry("y", 2.0);
  }

  @Test
  void aMapSmuggledUnderAPointShapedKeyIsStillRefused() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
        "MATCH (t:T) SET t.loc = {x: 1.0, y: 2.0, crs: 'cartesian', payload: {secret: 1}}")))
        .isInstanceOf(InvalidPropertyTypeException.class);
  }
}
