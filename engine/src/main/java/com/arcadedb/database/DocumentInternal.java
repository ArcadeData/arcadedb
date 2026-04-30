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

import com.arcadedb.utility.ExcludeFromJacocoGeneratedReport;

/**
 * Internal contract for {@link BaseDocument} that exposes serialization-format details to engine modules
 * (notably {@code com.arcadedb.serializer.BinarySerializer}) without leaking those details into the public
 * {@link Document} API. Code that consumes ArcadeDB as a library should never cast to this interface; cross-version
 * stability is not guaranteed.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@ExcludeFromJacocoGeneratedReport
public interface DocumentInternal {
  /**
   * Offset (in bytes) where the property header begins inside the record's binary buffer. Required by the
   * serializer to scan for {@code TYPE_EXTERNAL} pointers without re-parsing the bucket-level record framing.
   */
  int getPropertiesStartingPosition();
}
