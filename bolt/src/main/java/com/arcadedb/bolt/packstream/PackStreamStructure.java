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
package com.arcadedb.bolt.packstream;

import java.io.IOException;

/**
 * Interface for objects that can be serialized as PackStream structures.
 */
public interface PackStreamStructure {

  /**
   * Write this structure to a PackStream writer. This is the sole authority for the structure's wire
   * header (marker + signature) and body; there is no separate accessor for signature or field count,
   * because the header can be version-dependent and only the writer carries the negotiated Bolt version.
   */
  void writeTo(PackStreamWriter writer) throws IOException;
}
