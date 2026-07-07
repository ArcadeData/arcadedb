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
 * Bolt DateTime / DateTimeZoneId structure whose wire form depends on the negotiated protocol version.
 * Bolt 4.x uses the legacy signatures 'F'/'f' with the LOCAL epoch-second (wall clock treated as UTC).
 * Bolt 5.0+ uses 'I'/'i' with the true UTC epoch-second. Both epoch bases are computed at build time
 * from the source java.time value; writeTo selects by {@link PackStreamWriter#getBoltMajorVersion()}.
 */
public class BoltDateTimeStructure implements PackStreamStructure {
  private static final byte SIG_OFFSET_LEGACY = 0x46; // 'F' [secondsLocal, nanos, offsetSeconds]
  private static final byte SIG_ZONEID_LEGACY = 0x66; // 'f' [secondsLocal, nanos, zoneId]
  private static final byte SIG_OFFSET_UTC    = 0x49; // 'I' [secondsUtc,  nanos, offsetSeconds]
  private static final byte SIG_ZONEID_UTC    = 0x69; // 'i' [secondsUtc,  nanos, zoneId]

  private final boolean zoneId;
  private final long    localEpochSeconds;
  private final long    utcEpochSeconds;
  private final int     nanos;
  private final int     offsetSeconds; // used when zoneId == false
  private final String  zone;          // used when zoneId == true

  private BoltDateTimeStructure(final boolean zoneId, final long localEpochSeconds, final long utcEpochSeconds,
      final int nanos, final int offsetSeconds, final String zone) {
    this.zoneId = zoneId;
    this.localEpochSeconds = localEpochSeconds;
    this.utcEpochSeconds = utcEpochSeconds;
    this.nanos = nanos;
    this.offsetSeconds = offsetSeconds;
    this.zone = zone;
  }

  public static BoltDateTimeStructure offset(final long localEpochSeconds, final long utcEpochSeconds, final int nanos,
      final int offsetSeconds) {
    return new BoltDateTimeStructure(false, localEpochSeconds, utcEpochSeconds, nanos, offsetSeconds, null);
  }

  public static BoltDateTimeStructure zoneId(final long localEpochSeconds, final long utcEpochSeconds, final int nanos,
      final String zone) {
    return new BoltDateTimeStructure(true, localEpochSeconds, utcEpochSeconds, nanos, 0, zone);
  }

  @Override
  public byte getSignature() {
    // Legacy (Bolt 4.x) signature; writeTo is authoritative and emits the UTC signature ('I'/'i') when the writer negotiated Bolt >= 5.
    return zoneId ? SIG_ZONEID_LEGACY : SIG_OFFSET_LEGACY;
  }

  @Override
  public int getFieldCount() {
    return 3;
  }

  @Override
  public void writeTo(final PackStreamWriter writer) throws IOException {
    final boolean utc = writer.getBoltMajorVersion() >= 5;
    final long seconds = utc ? utcEpochSeconds : localEpochSeconds;
    if (zoneId) {
      writer.writeStructureHeader(utc ? SIG_ZONEID_UTC : SIG_ZONEID_LEGACY, 3);
      writer.writeInteger(seconds);
      writer.writeInteger(nanos);
      writer.writeString(zone);
    } else {
      writer.writeStructureHeader(utc ? SIG_OFFSET_UTC : SIG_OFFSET_LEGACY, 3);
      writer.writeInteger(seconds);
      writer.writeInteger(nanos);
      writer.writeInteger(offsetSeconds);
    }
  }
}
