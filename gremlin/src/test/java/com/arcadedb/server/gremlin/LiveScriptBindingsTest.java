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

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.utility.FileUtils;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.script.Bindings;
import javax.script.SimpleBindings;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The listing methods of {@link LiveScriptBindings} answer what its lookups answer (review of #9157): the manager's entry wins
 * over a base entry of the same name, a graph whose database is no longer open is not listed, and the base entry it shadowed
 * is visible again.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class LiveScriptBindingsTest {
  private ArcadeGraph       graph;
  private ArcadeGraphManager manager;
  private Bindings          base;
  private LiveScriptBindings live;

  @BeforeEach
  void setup() {
    graph = ArcadeGraph.open("./target/test-livescriptbindings");
    manager = new ArcadeGraphManager(new Settings());
    manager.putGraph("db", graph);
    base = new SimpleBindings();
    base.put("db", "baseValue");
    base.put("other", "otherValue");
    live = new LiveScriptBindings(base, manager);
  }

  @AfterEach
  void teardown() {
    if (graph.getDatabase().isOpen())
      graph.drop();
    else
      FileUtils.deleteRecursively(new File("./target/test-livescriptbindings"));
  }

  @Test
  void theManagerEntryWinsOverTheBaseEntry() {
    assertThat(live.get("db")).isSameAs(graph);
    assertThat(live.containsKey("db")).isTrue();
    assertThat(live.get("other")).isEqualTo("otherValue");
    assertThat(live).hasSize(2);
    assertThat(live.keySet()).containsExactlyInAnyOrder("db", "other");
    assertThat(live.values()).contains(graph, "otherValue");
  }

  @Test
  void aStaleManagerEntryLeavesTheBaseEntryVisibleEverywhere() {
    graph.close();

    assertThat(live.get("db")).isEqualTo("baseValue");
    assertThat(live.containsKey("db")).isTrue();
    assertThat(live).hasSize(2);
    assertThat(live.keySet()).containsExactlyInAnyOrder("db", "other");
    assertThat(live.values()).containsExactlyInAnyOrder("baseValue", "otherValue");
  }

  @Test
  void aStaleManagerEntryWithoutABaseEntryIsNotListed() {
    base.remove("db");
    graph.close();

    assertThat(live.get("db")).isNull();
    assertThat(live.containsKey("db")).isFalse();
    assertThat(live.keySet()).containsExactly("other");
    assertThat(live).hasSize(1);
  }
}
