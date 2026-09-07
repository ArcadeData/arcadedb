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
package com.arcadedb.server.gremlin;

import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #7230: every Gremlin query sent with the gremlinpython default {@code traversal_source} of {@code 'g'} logged
 * "Mapping 'g' alias to database '...'" at INFO. The mapping is a property of the deployment, not of the request - a
 * driver that never changes its traversal source resolves it identically on every query - so the note repeated once
 * per query and flooded the server log.
 * <p>
 * The report asked for DEBUG. The first occurrence is the useful one, though: it is what tells an operator which
 * database an unqualified traversal actually reaches, and demoting it outright would hide that from anyone who has
 * not raised the level in advance. So the announcement keeps INFO and only the repetitions are demoted, which is
 * what these tests pin down.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7230AliasMappingLogFloodTest {

  private ArcadeGraphManager newManager() {
    return new ArcadeGraphManager(new Settings());
  }

  /** The flood itself: the first query announces the mapping, the next thousand do not. */
  @Test
  void theAliasMappingIsAnnouncedOnceAndDemotedAfterwards() {
    final ArcadeGraphManager manager = newManager();

    assertThat(manager.announceAliasMappingLevel("graph"))
        .as("the first resolution tells the operator where an unqualified traversal lands")
        .isEqualTo(Level.INFO);

    for (int i = 0; i < 1_000; i++)
      assertThat(manager.announceAliasMappingLevel("graph"))
          .as("query %d repeats a mapping that has not changed", i)
          .isEqualTo(Level.FINE);
  }

  /**
   * The announcement is keyed on the RESOLVED database rather than latched once, so a mapping that starts pointing
   * somewhere else is announced again instead of changing silently under the operator.
   */
  @Test
  void aMappingOntoADifferentDatabaseIsAnnouncedAgain() {
    final ArcadeGraphManager manager = newManager();

    assertThat(manager.announceAliasMappingLevel("first")).isEqualTo(Level.INFO);
    assertThat(manager.announceAliasMappingLevel("second")).isEqualTo(Level.INFO);

    // ...and each of them is then quiet on its own.
    assertThat(manager.announceAliasMappingLevel("first")).isEqualTo(Level.FINE);
    assertThat(manager.announceAliasMappingLevel("second")).isEqualTo(Level.FINE);
  }

  /**
   * A database that goes away takes its announcement with it: should {@code 'g'} ever resolve back onto that name,
   * the operator is told rather than left with a silently re-established mapping.
   */
  @Test
  void removingTheGraphRearmsTheAnnouncement() {
    final ArcadeGraphManager manager = newManager();

    assertThat(manager.announceAliasMappingLevel("graph")).isEqualTo(Level.INFO);
    assertThat(manager.announceAliasMappingLevel("graph")).isEqualTo(Level.FINE);

    manager.removeGraph("graph");

    assertThat(manager.announceAliasMappingLevel("graph")).isEqualTo(Level.INFO);
  }

  /** Two managers do not share the state, so one server's announcement does not silence another's. */
  @Test
  void theAnnouncementIsPerManager() {
    assertThat(newManager().announceAliasMappingLevel("graph")).isEqualTo(Level.INFO);
    assertThat(newManager().announceAliasMappingLevel("graph")).isEqualTo(Level.INFO);
  }
}
