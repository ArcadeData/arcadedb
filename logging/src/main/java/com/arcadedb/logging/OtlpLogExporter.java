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
package com.arcadedb.logging;

import com.arcadedb.log.LogExporter;
import com.arcadedb.log.Logger;

/**
 * Builds the OTLP log decorator, and is how {@code LogManager} finds this module: declared in
 * {@code META-INF/services/com.arcadedb.log.LogExporter}, so it exists only when the jar does.
 *
 * @author Rui Pereira
 */
public class OtlpLogExporter implements LogExporter {

  /** The name {@code arcadedb.log.otlp.enabled} looks up. */
  private static final String NAME = "otlp";

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public Logger create(final Logger delegate, final String endpoint) {
    return new OtlpLogger(delegate, endpoint);
  }
}
