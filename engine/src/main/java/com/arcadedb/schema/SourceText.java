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

/**
 * A value a client sent as text without saying which type it meant, and which a protocol read as the richer value that text also
 * spells: a Postgres parameter of unspecified type that is a well-formed array literal is read as a list, because psycopg sends a
 * Python list exactly so (issue #2559), and a Python {@code str} exactly so too (issue #12711). Where a list is wanted it is that
 * list; converted to a STRING - stored into a STRING property, compared with a string - it is the text the client sent, never the
 * rendering of the value it was read as.
 */
public interface SourceText {
  /** The text the value was read from, exactly as the client sent it. */
  String sourceText();
}
