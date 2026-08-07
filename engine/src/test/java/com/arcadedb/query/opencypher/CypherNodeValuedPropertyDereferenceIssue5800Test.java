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

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5800: a node-valued property is dereferenceable in the writing
 * query (the assigned value is still the live {@code Vertex} instance) but, once persisted,
 * ArcadeDB stores it as a LINK/RID and reading the same property-access expression
 * ({@code holder.ref.id}) in a later transaction raised
 * {@code TypeError: Cannot access property 'id' on DatabaseRID value} instead of resolving
 * through the link, breaking transaction-boundary-independent semantics for the same expression.
 * <p>
 * ArcadeDB's native data model intentionally supports LINK properties backed by RIDs, so the
 * fix keeps the node-to-LINK conversion but makes the persisted RID transparently dereferenceable
 * through chained property access, exactly like the in-transaction Vertex value was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherNodeValuedPropertyDereferenceIssue5800Test {
  private Database database;

  @BeforeEach
  void setup() {
    database = new DatabaseFactory("./target/databases/cyphernodevaluedprop5800").create();
  }

  @AfterEach
  void teardown() {
    if (database != null)
      database.drop();
  }

  @Test
  void chainedPropertyAccessOnPersistedLinkDereferencesAfterCommit() {
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher",
          "CREATE (holder:T {role: 'holder'}), (target:T {role: 'target', id: 42}) " +
              "SET holder.ref = target " +
              "RETURN holder.ref AS ref, holder.ref.id AS referencedId");
      assertThat(rs.hasNext()).isTrue();
      final Result row = rs.next();
      // Within the writing query the assigned value is still the live Vertex.
      assertThat(((Number) row.getProperty("referencedId")).intValue()).isEqualTo(42);
    });

    // New transaction: the property was persisted as a LINK/RID.
    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) RETURN holder.ref.id AS referencedId");
      assertThat(rs.hasNext()).isTrue();
      final Object referencedId = rs.next().getProperty("referencedId");
      assertThat(referencedId).as("dereferencing a persisted node-valued property must not fail").isNotNull();
      assertThat(((Number) referencedId).intValue()).isEqualTo(42);
    });
  }

  @Test
  void chainedPropertyAccessOnPersistedLinkRestoresTemporalType() {
    // A temporal (Duration) property on the dereferenced target is stored as an ISO-8601
    // String (ArcadeDB has no native binary Duration type). The chained-access path must
    // restore it to a CypherDuration - exactly like the single-level path already does via
    // PropertyAccessExpression.convertFromStorage() - so a further component access
    // (.seconds) resolves instead of failing on a plain String.
    database.transaction(() -> database.command("opencypher",
        "CREATE (holder:T {role: 'holder'}), (target:T {role: 'target', dur: duration('PT2H30M')}) " +
            "SET holder.ref = target RETURN holder").close());

    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) RETURN holder.ref.dur.seconds AS totalSeconds");
      assertThat(rs.hasNext()).isTrue();
      final Object totalSeconds = rs.next().getProperty("totalSeconds");
      assertThat(totalSeconds).as("Duration property must be restored, not left as a raw String").isNotNull();
      assertThat(((Number) totalSeconds).longValue()).isEqualTo(2 * 3600 + 30 * 60);
    });
  }

  @Test
  void directPropertyAccessOnPersistedLinkStillDereferences() {
    // Control: the single-level (variable-bound) property access path already handled RIDs
    // before this fix. Kept here so a future regression on that path is also caught.
    database.transaction(() -> database.command("opencypher",
        "CREATE (holder:T {role: 'holder'}), (target:T {role: 'target', id: 7}) " +
            "SET holder.ref = target RETURN holder").close());

    database.transaction(() -> {
      final ResultSet rs = database.query("opencypher",
          "MATCH (holder:T {role: 'holder'}) WITH holder.ref AS r RETURN r.id AS referencedId");
      assertThat(rs.hasNext()).isTrue();
      final Object referencedId = rs.next().getProperty("referencedId");
      assertThat(((Number) referencedId).intValue()).isEqualTo(7);
    });
  }
}
