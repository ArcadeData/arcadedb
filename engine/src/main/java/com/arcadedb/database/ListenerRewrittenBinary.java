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
package com.arcadedb.database;

/**
 * The content of a record whose after-read listener handed back other content than the stored one (a decryption, for instance),
 * which remembers the stored image the record was read as (issue #12579).
 * <p>
 * A record answers its properties from {@link BaseRecord#getBuffer() its buffer}, and that is the content the listener returned.
 * What a write or a delete has to compare with the slot, or to clean the indexes and the EXTERNAL values up from, is what the
 * bucket holds: the image the record was read as. Comparing the returned content with the slot told a record no other transaction
 * touched from a changed one never, and every delete and update of such a record was refused as a concurrent modification.
 * <p>
 * The image rides on the buffer rather than on a field of the record, so the records nobody rewrote (all of them but the ones read
 * under such a listener) pay nothing for it, and it goes away with the buffer: whatever replaces the content of the record, a save
 * or a reload that runs no listener, replaces the image with it. {@link #copyOfContent()} is not overridden on purpose: a copy of
 * this content that is written to a page, or handed to another record, must not carry the image of a state the slot has left.
 * {@link #copyOf(Binary)} is the explicit way of the one caller that wants it carried, the copy {@code modify()} builds.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
final class ListenerRewrittenBinary extends Binary {
  private final Binary storedImage;

  private ListenerRewrittenBinary(final Binary content, final Binary storedImage) {
    // THE ARRAY IS NOT COPIED: THE CALLER PASSES CONTENT THAT IS ALREADY ITS OWN (getNotReusable() OR copyOfContent())
    super(content.getContent(), content.size());
    this.autoResizable = content.autoResizable;
    this.storedImage = storedImage;
  }

  /**
   * @param content     the content the listener returned, serialized, not reusable and not shared with anything else
   * @param storedImage the image the record was read as, which this keeps as it is: the caller hands over a private copy
   */
  static Binary of(final Binary content, final Binary storedImage) {
    return new ListenerRewrittenBinary(content, storedImage);
  }

  /**
   * A copy of {@code content}, with its own copy of the stored image when {@code content} carries one. For the {@code
   * MutableDocument} {@code modify()} builds, which compares its stored image with the slot where it is saved or deleted.
   */
  static Binary copyOf(final Binary content) {
    final Binary copy = content.copyOfContent();
    return content instanceof ListenerRewrittenBinary rewritten ? new ListenerRewrittenBinary(copy,
        rewritten.storedImage.copyOfContent()) : copy;
  }

  Binary getStoredImage() {
    return storedImage;
  }
}
