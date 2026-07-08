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
package com.arcadedb.bolt.structure;

import com.arcadedb.bolt.packstream.PackStreamStructure;
import com.arcadedb.bolt.packstream.PackStreamWriter;

import java.io.IOException;

/**
 * A Bolt spatial Point PackStream structure. Point2D (signature 0x58) carries [srid, x, y];
 * Point3D (signature 0x59) carries [srid, x, y, z]. srid is an integer; coordinates are doubles.
 */
public class BoltPointStructure implements PackStreamStructure {
  public static final byte SIGNATURE_2D = 0x58;
  public static final byte SIGNATURE_3D = 0x59;

  private final int    srid;
  private final double x;
  private final double y;
  private final Double z; // null for 2D

  public BoltPointStructure(final int srid, final double x, final double y) {
    this(srid, x, y, null);
  }

  public BoltPointStructure(final int srid, final double x, final double y, final Double z) {
    this.srid = srid;
    this.x = x;
    this.y = y;
    this.z = z;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    final byte signature = z == null ? SIGNATURE_2D : SIGNATURE_3D;
    final int fieldCount = z == null ? 3 : 4;
    writer.writeStructureHeader(signature, fieldCount);
    writer.writeValue((long) srid);
    writer.writeValue(x);
    writer.writeValue(y);
    if (z != null)
      writer.writeValue(z);
  }

  public int getSrid() {
    return srid;
  }

  public double getX() {
    return x;
  }

  public double getY() {
    return y;
  }

  public Double getZ() {
    return z;
  }
}
