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

import com.arcadedb.engine.TrackableContent;
import com.arcadedb.exception.ArcadeDBException;

import java.nio.*;

public class TrackableBinary extends Binary implements TrackableContent {
  private final TrackableContent derivedFrom;

  public TrackableBinary(final TrackableContent derivedFrom, final ByteBuffer slice) {
    super(slice);
    this.derivedFrom = derivedFrom;
  }

  public int[] getModifiedRange() {
    return derivedFrom.getModifiedRange();
  }

  public void updateModifiedRange(final int start, final int end) {
    derivedFrom.updateModifiedRange(buffer.arrayOffset() + start, buffer.arrayOffset() + end);
  }

  @Override
  protected void checkForAllocation(final int offset, final int bytesToWrite) {
    super.checkForAllocation(offset, bytesToWrite);
    updateModifiedRange(offset, offset + bytesToWrite - 1);
  }

  public void move(final int startPosition, final int destPosition, final int length) {
    super.move(startPosition, destPosition, length);
    updateModifiedRange(startPosition, destPosition + length);
  }

  public Binary slice() {
    return new TrackableBinary(this, buffer.slice());
  }

  public Binary slice(final int position) {
    try {
      // THIS WILL NOT BE NECESSARY AFTER SWITCHING TO JKD13 (https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-5029431)
      return (Binary) executeInLock(() -> {
        buffer.position(position);
        return new TrackableBinary(this, buffer.slice());
      });
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new ArcadeDBException("Cannot slice the buffer " + this, e);
    }
  }

  public Binary slice(final int position, final int length) {
    try {
      // THIS WILL NOT BE NECESSARY AFTER SWITCHING TO JKD13 (https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-5029431)
      final ByteBuffer result = (ByteBuffer) executeInLock(() -> {
        buffer.position(position);
        return buffer.slice();
      });
      result.position(length);
      result.flip();
      return new TrackableBinary(this, result);
    } catch (final RuntimeException e) {
      throw e;
    } catch (final Exception e) {
      throw new ArcadeDBException("Cannot slice the buffer " + this, e);
    }
  }
}
