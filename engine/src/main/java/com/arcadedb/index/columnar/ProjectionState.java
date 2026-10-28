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
package com.arcadedb.index.columnar;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * One immutable published state of a bucket's projection: its segment readers (with their dead sets), its overlay views, the
 * dictionary generation they are encoded with and the names of the files it reads. Ref-counted: the projection holds the first
 * reference while the state is current, and every snapshot a query acquires holds one more.
 * <p>
 * The state holds no path and deletes nothing (R2). On the last {@link #release()} it gives back the {@link SegmentFile} references
 * it took and calls {@code onLastRelease} once; the owner ({@code ColumnarProjection}) then deletes the files that neither the current
 * manifest nor a still-retained state's {@link #files()} names. It never closes a {@link SegmentReader}: readers are shared by
 * consecutive states and closed by the projection when no retained state names them (V-M8).
 */
final class ProjectionState {
  private final long                          generation;
  private final SegmentReader[]               segments;
  private final ColumnarSegmentView[]         overlays;
  private final Map<String, GlobalDictionary> dictionaries;
  private final Set<String>                   files;
  private final boolean                       usable;
  private final Consumer<ProjectionState>     onLastRelease;
  private final AtomicInteger                 references = new AtomicInteger(1);

  /**
   * Takes one reference on the {@link SegmentFile} of every segment, and starts with one reference of its own (the owner's).
   *
   * @param segments      shared, never written afterwards
   * @param overlays      shared, never written afterwards
   * @param dictionaries  the generation of each property's dictionary the segments and overlays are encoded with
   * @param files         names of every segment, dead set, dictionary and owner map file this state reads (V-B7), from the manifest
   *                      the state was built from
   * @param onLastRelease called once, when the reference count reaches 0
   *
   * @throws IllegalStateException when a segment's file is already released; no reference is kept then
   */
  ProjectionState(final long generation, final SegmentReader[] segments, final ColumnarSegmentView[] overlays,
      final Map<String, GlobalDictionary> dictionaries, final Set<String> files, final boolean usable,
      final Consumer<ProjectionState> onLastRelease) {
    for (int i = 0; i < segments.length; i++) {
      try {
        segments[i].file().retain();
      } catch (final RuntimeException e) {
        for (int j = 0; j < i; j++)
          segments[j].file().release();
        throw e;
      }
    }
    this.generation = generation;
    this.segments = segments;
    this.overlays = overlays;
    this.dictionaries = dictionaries;
    this.files = files;
    this.usable = usable;
    this.onLastRelease = onLastRelease;
  }

  /**
   * Adds a reference.
   *
   * @throws IllegalStateException when the state was already released for the last time
   */
  ProjectionState retain() {
    while (true) {
      final int current = references.get();
      if (current <= 0)
        throw new IllegalStateException("Projection state of generation " + generation + " is already released");
      if (references.compareAndSet(current, current + 1))
        return this;
    }
  }

  /**
   * Drops a reference. The last one releases the segment file references taken by the constructor, then calls
   * {@code onLastRelease}.
   *
   * @throws IllegalStateException when released more often than retained
   */
  void release() {
    while (true) {
      final int current = references.get();
      if (current <= 0)
        throw new IllegalStateException("Projection state of generation " + generation + " is released more often than retained");
      if (references.compareAndSet(current, current - 1)) {
        if (current == 1) {
          for (final SegmentReader segment : segments)
            segment.file().release();
          onLastRelease.accept(this);
        }
        return;
      }
    }
  }

  long generation() {
    return generation;
  }

  /** The segment readers, oldest first; shared, never write the array. */
  SegmentReader[] segments() {
    return segments;
  }

  /** The overlay views, the frozen overlay first, then the live one; shared, never write the array. */
  ColumnarSegmentView[] overlays() {
    return overlays;
  }

  /** The dictionary generation of the property, null when the state has none for it. */
  GlobalDictionary dictionary(final String property) {
    return dictionaries.get(property);
  }

  /** The dictionary generation of every property, as given to the constructor; immutable when the caller passed an immutable map. */
  Map<String, GlobalDictionary> dictionaries() {
    return dictionaries;
  }

  /** The constructor's file names (V-B7). */
  Set<String> files() {
    return files;
  }

  boolean isUsable() {
    return usable;
  }

  int referencesForTesting() {
    return references.get();
  }
}
