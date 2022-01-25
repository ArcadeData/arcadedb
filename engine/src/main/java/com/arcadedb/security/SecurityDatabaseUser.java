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
package com.arcadedb.security;

/**
 * Security user for a database. It declares the authorized permissions against the database.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
public interface SecurityDatabaseUser {
  enum ACCESS {
    CREATE_RECORD("createRecord", "create records"),//
    READ_RECORD("readRecord", "read records"),//
    UPDATE_RECORD("updateRecord", "update records"),//
    DELETE_RECORD("deletedRecord", "delete records");

    public final String name;
    public final String fullName;

    ACCESS(final String name, final String fullName) {
      this.name = name;
      this.fullName = fullName;
    }
  }

  enum DATABASE_ACCESS {
    UPDATE_SECURITY("updateSecurity", "update security"),//
    UPDATE_SCHEMA("updateSchema", "update schema"),//
    UPDATE_DATABASE_SETTINGS("updateDatabaseSettings", "update database settings");

    public final String name;
    public final String fullName;

    DATABASE_ACCESS(final String name, final String fullName) {
      this.name = name;
      this.fullName = fullName;
    }

    public static DATABASE_ACCESS getByName(final String name) {
      for (DATABASE_ACCESS v : DATABASE_ACCESS.values())
        if (v.name.equals(name))
          return v;
      return null;
    }
  }

  boolean requestAccessOnDatabase(DATABASE_ACCESS access);

  boolean requestAccessOnFile(int fileId, ACCESS access);

  String getName();

  long getResultSetLimit();

  long getReadTimeout();
}
