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
package com.arcadedb.integration.importer;

/**
 * A source of zero bytes, whatever its format. Its own type because {@link Importer} answers it by what else the import names:
 * beside a source that is not empty it is skipped with a warning, otherwise it fails the import (issue #11311).
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public class EmptySourceException extends ImportException {
  public EmptySourceException(final String url) {
    super("Cannot import the source '" + url + "': it is empty");
  }
}
