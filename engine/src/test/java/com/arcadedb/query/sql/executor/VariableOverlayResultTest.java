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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12723: {@link VariableOverlayResult} answers what the copy of the enclosing row it replaces answered - the enclosing
 * columns, the bound variables over them, the second binding over the first - without copying anything.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class VariableOverlayResultTest {

  @Test
  void answersTheEnclosingColumnsAndTheBoundVariableOverThem() {
    final ResultInternal outer = row("a", 1, "x", 2);
    final VariableOverlayResult overlay = new VariableOverlayResult(outer, "x").bind(10);

    assertThat(overlay.<Object>getProperty("a")).isEqualTo(1);
    assertThat(overlay.<Object>getProperty("x")).isEqualTo(10);
    assertThat(overlay.<Object>getProperty("x", "default")).isEqualTo(10);
    assertThat(overlay.<Object>getProperty("missing")).isNull();
    assertThat(overlay.<Object>getProperty("missing", "default")).isEqualTo("default");
    assertThat(overlay.getPropertyIfPresent("missing", "absent")).isEqualTo("absent");
    assertThat(overlay.hasProperty("a")).isTrue();
    assertThat(overlay.hasProperty("x")).isTrue();
    assertThat(overlay.hasProperty("missing")).isFalse();
    assertThat(overlay.listsProperty("missing")).isFalse();
    // The enclosing row is read, never written
    assertThat(outer.<Object>getProperty("x")).isEqualTo(2);
  }

  @Test
  void rebindingReplacesTheValueOfTheSameRow() {
    final VariableOverlayResult overlay = new VariableOverlayResult(row("a", 1), "x");
    assertThat(overlay.bind(1)).isSameAs(overlay);
    assertThat(overlay.<Object>getProperty("x")).isEqualTo(1);
    overlay.bind(2);
    assertThat(overlay.<Object>getProperty("x")).isEqualTo(2);
    overlay.bind(null);
    assertThat(overlay.<Object>getProperty("x")).isNull();
    // A variable bound to null still exists, as on the copy
    assertThat(overlay.hasProperty("x")).isTrue();
    assertThat(overlay.<Object>getProperty("x", "default")).isNull();
  }

  @Test
  void theSecondBindingShadowsTheFirstOfTheSameName() {
    final VariableOverlayResult overlay = new VariableOverlayResult(row("s", 1000), "s", "s").bind(1, 2);
    assertThat(overlay.<Object>getProperty("s")).isEqualTo(2);
    overlay.bindSecond(3);
    assertThat(overlay.<Object>getProperty("s")).isEqualTo(3);
    assertThat(overlay.getPropertyNames()).containsExactly("s");
  }

  @Test
  void aNullNameBindsNothingAndLeavesTheEnclosingColumn() {
    final VariableOverlayResult overlay = new VariableOverlayResult(row("acc", 5, "x", 7), null, "acc").bind(99, 6);
    assertThat(overlay.<Object>getProperty("acc")).isEqualTo(6);
    assertThat(overlay.<Object>getProperty("x")).isEqualTo(7);
    assertThat(overlay.getPropertyNames()).containsExactly("acc", "x");
  }

  @Test
  void namesFollowTheOrderOfTheCopy() {
    // The copy put the enclosing names first, in their order, then the variables; a variable that shadows an enclosing
    // column keeps that column's place
    final VariableOverlayResult overlay = new VariableOverlayResult(row("b", 1, "a", 2), "z", "a").bind(3, 4);
    assertThat(overlay.getPropertyNames()).containsExactly("b", "a", "z");
    assertThat(overlay.toMap()).containsExactly(Map.entry("b", 1), Map.entry("a", 4), Map.entry("z", 3));
    assertThat(overlay.toString()).isEqualTo("{b=1, a=4, z=3}");
  }

  @Test
  void nestedOverlaysSeeEveryEnclosingBinding() {
    final VariableOverlayResult outer = new VariableOverlayResult(row("base", 100), "x").bind(1);
    final VariableOverlayResult inner = new VariableOverlayResult(outer, "y").bind(10);
    assertThat(inner.<Object>getProperty("base")).isEqualTo(100);
    assertThat(inner.<Object>getProperty("x")).isEqualTo(1);
    assertThat(inner.<Object>getProperty("y")).isEqualTo(10);
    assertThat(inner.getPropertyNames()).containsExactly("base", "x", "y");
    // The inner row reads the binding the outer one holds now
    outer.bind(2);
    assertThat(inner.<Object>getProperty("x")).isEqualTo(2);
  }

  @Test
  void withoutAnEnclosingRowOnlyTheVariablesExist() {
    final VariableOverlayResult overlay = new VariableOverlayResult(null, "x").bind(List.of(1, 2));
    assertThat(overlay.<Object>getProperty("x")).isEqualTo(List.of(1, 2));
    assertThat(overlay.<Object>getProperty("y")).isNull();
    assertThat(overlay.<Object>getProperty("y", "default")).isEqualTo("default");
    assertThat(overlay.getPropertyNames()).containsExactly("x");
  }

  @Test
  void aNameTheEnclosingRowDoesNotListReadsNullAsOnTheCopy() {
    // An enclosing row that resolves a name it does not list, as a row reading through a record can: the copy held only the
    // listed names, so every getter answers as if the name were not there
    final ResultInternal outer = new ResultInternal(Map.of("a", 1)) {
      @Override
      public <T> T getProperty(final String name) {
        return "hidden".equals(name) ? (T) "resolved" : super.getProperty(name);
      }
    };
    final VariableOverlayResult overlay = new VariableOverlayResult(outer, "x").bind(1);
    assertThat(overlay.<Object>getProperty("a")).isEqualTo(1);
    assertThat(overlay.<Object>getProperty("hidden")).isNull();
    assertThat(overlay.<Object>getProperty("hidden", "default")).isEqualTo("default");
    assertThat(overlay.hasProperty("hidden")).isFalse();
  }

  @Test
  void isAProjectionWithNoRecordMetadataOrDatabase() {
    final VariableOverlayResult overlay = new VariableOverlayResult(row("a", 1), "x").bind(1);
    assertThat(overlay.isProjection()).isTrue();
    assertThat(overlay.isElement()).isFalse();
    assertThat(overlay.getElement()).isEqualTo(Optional.empty());
    assertThat(overlay.getRecord()).isEqualTo(Optional.empty());
    assertThat(overlay.toElement()).isNull();
    assertThat(overlay.getIdentity()).isEqualTo(Optional.empty());
    assertThat(overlay.getMetadataKeys()).isEmpty();
    assertThat(overlay.getMetadata("any")).isNull();
    assertThat(overlay.getDatabase()).isNull();
    assertThat(overlay.getElementProperty("a")).isNull();
  }

  private static ResultInternal row(final Object... pairs) {
    final ResultInternal row = new ResultInternal();
    for (int i = 0; i < pairs.length; i += 2)
      row.setProperty((String) pairs[i], pairs[i + 1]);
    return row;
  }
}
