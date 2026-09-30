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
  private volatile AtomicReferenceArray<Component> slots = new AtomicReferenceArray<>(16);
  private volatile int                              size;

  Component get(final int id) {
    final AtomicReferenceArray<Component> current = slots;
    return id >= 0 && id < current.length() ? current.get(id) : null;
  }

  /** Number of slots in use (one past the highest index ever added or set). */
  int size() {
    return size;
  }

  synchronized void set(final int id, final Component component) {
    ensureSize(id + 1);
    slots.set(id, component);
  }

  synchronized void add(final Component component) {
    final int id = size;
    ensureSize(id + 1);
    slots.set(id, component);
  }

  synchronized void clear() {
    slots = new AtomicReferenceArray<>(16);
    size = 0;
  }

  /** Atomically publishes a whole new table. */
  synchronized void replaceAll(final Component[] components) {
    final AtomicReferenceArray<Component> next = new AtomicReferenceArray<>(Math.max(16, components.length));
    for (int i = 0; i < components.length; i++)
      next.set(i, components[i]);
    slots = next;
    size = components.length;
  }

  /** Copy of the current content, {@link #size()} long. */
  Component[] toArray() {
    final AtomicReferenceArray<Component> current = slots;
    final int s = Math.min(size, current.length());
    final Component[] copy = new Component[s];
    for (int i = 0; i < s; i++)
      copy[i] = current.get(i);
    return copy;
  }

  List<Component> toList() {
    final Component[] array = toArray();
    final List<Component> list = new ArrayList<>(array.length);
    for (final Component c : array)
      list.add(c);
    return list;
  }

  private void ensureSize(final int required) {
    if (required > slots.length()) {
      final AtomicReferenceArray<Component> next = new AtomicReferenceArray<>(Math.max(required, slots.length() * 2));
      final AtomicReferenceArray<Component> old = slots;
      for (int i = 0; i < old.length(); i++)
        next.set(i, old.get(i));
      slots = next;
    }
    if (required > size)
      size = required;
  }
}
