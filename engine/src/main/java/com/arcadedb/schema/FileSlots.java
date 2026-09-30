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
package com.arcadedb.schema;

import com.arcadedb.engine.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * File-id indexed table of the schema components (issue #8634). Reads are lock-free: {@link #get(int)} is a volatile
 * read of the current array plus a volatile element read, so the per-record bucket resolution of every query thread no
 * longer serializes on one monitor. Writers are expected to hold the monitor of this object (<code>synchronized
 * (files)</code>) when they need a compound read-modify-write; a single mutator is safe on its own. Growth and
 * {@link #replaceAll(Component[])} publish a whole new array, so a reader never sees a half rewritten table.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
final class FileSlots {
  /** The array and its used length travel together, so a reader can never pair one generation's array with another's size. */
  private record Table(AtomicReferenceArray<Component> slots, int size) {
  }

  private volatile Table table = new Table(new AtomicReferenceArray<>(16), 0);

  Component get(final int id) {
    final Table current = table;
    return id >= 0 && id < current.size ? current.slots.get(id) : null;
  }

  /** Number of slots in use (one past the highest index ever added or set). */
  int size() {
    return table.size;
  }

  synchronized void set(final int id, final Component component) {
    ensureSize(id + 1).slots.set(id, component);
  }

  synchronized void add(final Component component) {
    final int id = table.size;
    ensureSize(id + 1).slots.set(id, component);
  }

  synchronized void clear() {
    table = new Table(new AtomicReferenceArray<>(16), 0);
  }

  /** Atomically publishes a whole new table. */
  synchronized void replaceAll(final Component[] components) {
    final AtomicReferenceArray<Component> next = new AtomicReferenceArray<>(Math.max(16, components.length));
    for (int i = 0; i < components.length; i++)
      next.set(i, components[i]);
    table = new Table(next, components.length);
  }

  /** Copy of the current content, {@link #size()} long. */
  Component[] toArray() {
    final Table current = table;
    final Component[] copy = new Component[current.size];
    for (int i = 0; i < copy.length; i++)
      copy[i] = current.slots.get(i);
    return copy;
  }

  List<Component> toList() {
    return new ArrayList<>(Arrays.asList(toArray()));
  }

  /** First component with this name, scanning one consistent generation of the table without copying it. */
  Component findByName(final String name) {
    final Table current = table;
    for (int i = 0; i < current.size; i++) {
      final Component c = current.slots.get(i);
      if (c != null && name.equals(c.getName()))
        return c;
    }
    return null;
  }

  private Table ensureSize(final int required) {
    Table current = table;
    if (required <= current.size)
      return current;

    AtomicReferenceArray<Component> slots = current.slots;
    if (required > slots.length()) {
      final AtomicReferenceArray<Component> next = new AtomicReferenceArray<>(Math.max(required, slots.length() * 2));
      for (int i = 0; i < current.size; i++)
        next.set(i, slots.get(i));
      slots = next;
    }
    current = new Table(slots, required);
    table = current;
    return current;
  }
}
