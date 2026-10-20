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
package com.arcadedb.utility;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * #10377: {@link MultiIterator} and {@link InterleavedIterator} are what a promoted super-node serves every edge list
 * walk through, and both refused {@code remove()}. They now hand it to the source that produced the last element.
 */
class Issue10377CompositeIteratorRemoveTest {

  @Test
  void multiIteratorRemovesFromTheSourceOfTheLastElement() {
    final List<String> a = new ArrayList<>(List.of("a1", "a2"));
    final List<String> b = new ArrayList<>(List.of("b1", "b2"));
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(a.iterator());
    it.addIterator(b.iterator());

    final List<String> seen = new ArrayList<>();
    while (it.hasNext()) {
      final String value = it.next();
      seen.add(value);
      if (value.equals("a2") || value.equals("b1"))
        it.remove();
    }

    assertThat(seen).containsExactly("a1", "a2", "b1", "b2");
    assertThat(a).containsExactly("a1");
    assertThat(b).containsExactly("b2");
  }

  @Test
  void multiIteratorRemoveAfterAHasNextThatMovedToTheNextSourceStillRemovesTheReturnedElement() {
    final List<String> a = new ArrayList<>(List.of("a1"));
    final List<String> b = new ArrayList<>(List.of("b1"));
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(a.iterator());
    it.addIterator(b.iterator());

    assertThat(it.next()).isEqualTo("a1");
    // The peek moves the cursor onto the second source: the removal is still about "a1"
    assertThat(it.hasNext()).isTrue();
    it.remove();

    assertThat(a).isEmpty();
    assertThat(b).containsExactly("b1");
    assertThat(it.next()).isEqualTo("b1");
  }

  @Test
  void multiIteratorRemoveBeforeNextIsIllegalState() {
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(new ArrayList<>(List.of("a1")).iterator());
    assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void multiIteratorSecondRemoveForTheSameElementIsIllegalState() {
    final List<String> a = new ArrayList<>(List.of("a1", "a2"));
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(a.iterator());
    it.next();
    it.remove();
    assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
    assertThat(a).containsExactly("a2");
  }

  @Test
  void interleavedIteratorSecondRemoveForTheSameEntryIsIllegalState() {
    final List<String> a = new ArrayList<>(List.of("a1", "a2"));
    @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { a.iterator() };
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources);
    it.next();
    it.remove();
    assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
    assertThat(a).containsExactly("a2");
  }

  @Test
  void multiIteratorRemoveAfterResetIsIllegalState() {
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(new ArrayList<>(List.of("a1", "a2")));
    it.next();
    it.reset();
    assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void multiIteratorKeepsTheSourceWhenItRefusesTheRemoval() {
    final Iterator<String> refusing = List.of("a1").iterator();
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(refusing);
    it.next();
    assertThatThrownBy(it::remove).isInstanceOf(UnsupportedOperationException.class);
    // A caller that knows the source better (CHECK DATABASE's keep-the-edge prune) can still reach it
    assertThat(it.getLastReturnedSource()).isSameAs(refusing);
  }

  @Test
  void multiIteratorOverACollectionSourceRemovesFromIt() {
    final List<String> a = new ArrayList<>(List.of("a1", "a2"));
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(a);
    it.next();
    it.remove();
    assertThat(a).containsExactly("a2");
  }

  @Test
  void interleavedIteratorRemovesFromTheSourceOfTheLastElement() {
    final List<String> a = new ArrayList<>(List.of("a1", "a2"));
    final List<String> b = new ArrayList<>(List.of("b1", "b2"));
    @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { a.iterator(), b.iterator() };
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources);

    final List<String> seen = new ArrayList<>();
    while (it.hasNext()) {
      final String value = it.next();
      seen.add(value);
      if (value.equals("b1") || value.equals("a2"))
        it.remove();
    }

    assertThat(seen).containsExactly("a1", "b1", "a2", "b2");
    assertThat(a).containsExactly("a1");
    assertThat(b).containsExactly("b2");
  }

  @Test
  void interleavedIteratorRemoveBeforeNextIsIllegalState() {
    @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { new ArrayList<>(List.of("a1")).iterator() };
    final InterleavedIterator<String> it = new InterleavedIterator<>(sources);
    assertThatThrownBy(it::remove).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void nestedInterleavedInsideMultiRemoves() {
    final List<String> a = new ArrayList<>(List.of("a1"));
    final List<String> b = new ArrayList<>(List.of("b1"));
    @SuppressWarnings("unchecked") final Iterator<String>[] sources = new Iterator[] { a.iterator(), b.iterator() };
    final MultiIterator<String> it = new MultiIterator<>();
    it.addIterator(new InterleavedIterator<>(sources));

    assertThat(it.next()).isEqualTo("a1");
    assertThat(it.next()).isEqualTo("b1");
    it.remove();
    assertThat(a).containsExactly("a1");
    assertThat(b).isEmpty();
  }
}
