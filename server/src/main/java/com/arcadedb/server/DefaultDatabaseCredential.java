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
package com.arcadedb.server;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * One {@code user[:password[:group]]} credential of a {@code arcadedb.server.defaultDatabases} entry (issue #11269).
 * <p>
 * {@code ':'} separates the fields, {@code ','} the credentials, {@code ';'} the entries and {@code '['}/{@code ']'} the
 * block, so none of them can appear raw inside a field. A field that needs one writes it percent-encoded, as the userinfo
 * of a URI does: {@code mydb[myuser:my%3Apass:admin]} is the user {@code myuser} with the password {@code my:pass} in the
 * group {@code admin}. Only a {@code '%'} followed by two hexadecimal digits is decoded, so a password that merely contains
 * a {@code '%'} reads as it always did; {@code %25} spells a {@code '%'} that is followed by two hexadecimal digits. A
 * {@code '+'} stays a {@code '+'}: this is not form encoding.
 * <p>
 * Encoding is what keeps the separators unambiguous in the RAW text, which is what lets
 * {@code GlobalConfiguration.publishableValue} redact the password positionally without decoding anything: the field it
 * masks is exactly the field this class decodes into the password.
 * <p>
 * A credential with more than three fields used to be read as {@code user:password:group} with the rest dropped, so a
 * password written with a raw {@code ':'} was truncated at it and the text after it became the group: the password
 * {@code s3cret:admin} created a user in the {@code admin} group with the password {@code s3cret}. Such a credential is now
 * refused, because whichever of its fields is the password cannot be told. So is a credential with an empty user name
 * ({@code :pw}) or an empty password ({@code user:}, {@code user::group}): the first used to be read as a reference to a
 * user named {@code "user:"}, the second created a user anybody could authenticate as.
 * <p>
 * Every field is decoded, the user name and the group as well as the password, so the three spell a separator the same
 * way. The compatibility cost is the same for each: a name written before with a literal {@code '%'} followed by two
 * hexadecimal digits ({@code team%41}) now reads as the decoded name ({@code teamA}) and so names another user or group;
 * such a {@code '%'} is written {@code %25}. A {@code '%'} followed by anything else is read as written, as before.
 *
 * @param user     the user name, decoded
 * @param password the password, decoded, or {@code null} for a credential that only names an existing user
 * @param group    the group, decoded, or {@code null} when none is written
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
record DefaultDatabaseCredential(String user, String password, String group) {

  /**
   * Parses one credential.
   *
   * @throws IllegalArgumentException when the credential cannot be read unambiguously. The message never quotes the
   *                                  password, nor any field that could be part of one: it goes to the server log.
   */
  static DefaultDatabaseCredential parse(final String credential) {
    final String[] fields = credential.split(":", -1);
    if (fields.length > 3)
      throw new IllegalArgumentException(
          "has " + fields.length + " ':'-separated fields where the format reads at most 3 (user:password[:group]): a ':' inside the "
              + "password or the group must be written as %3A");

    final String user = decode(fields[0], "user name");
    if (user.isEmpty())
      throw new IllegalArgumentException("has no user name");
    refuseControlCharacters(user, "user name");

    if (fields.length == 1)
      return new DefaultDatabaseCredential(user, null, null);

    final String password = decode(fields[1], "password");
    if (password.isEmpty())
      throw new IllegalArgumentException("has an empty password");

    final String group = fields.length == 3 && !fields[2].isEmpty() ? decode(fields[2], "group") : null;
    if (group != null)
      refuseControlCharacters(group, "group");
    return new DefaultDatabaseCredential(user, password, group);
  }

  /**
   * A name is quoted in the server log and stored in the users file, so a control character an escape can now spell
   * ({@code %0A}, {@code %00}) is refused in it: it would let one credential write lines of its own into the log. A password
   * is never logged and may hold any character.
   */
  private static void refuseControlCharacters(final String name, final String fieldName) {
    for (int i = 0; i < name.length(); i++)
      if (Character.isISOControl(name.charAt(i)))
        throw new IllegalArgumentException("has a " + fieldName + " with a control character in it");
  }

  /**
   * Decodes every {@code %XX} sequence of {@code field} as UTF-8 and leaves everything else, a {@code '%'} not followed by
   * two hexadecimal digits included, as written.
   *
   * @throws IllegalArgumentException when the decoded bytes are not valid UTF-8
   */
  static String decode(final String field, final String fieldName) {
    final int first = field.indexOf('%');
    if (first < 0)
      return field;

    final StringBuilder decoded = new StringBuilder(field.length());
    decoded.append(field, 0, first);
    final byte[] bytes = new byte[field.length() / 3];
    int i = first;
    while (i < field.length()) {
      if (!isEscape(field, i)) {
        decoded.append(field.charAt(i++));
        continue;
      }
      // A run of escapes is one byte sequence: a multi-byte character is spelled as several of them
      int length = 0;
      while (isEscape(field, i)) {
        bytes[length++] = (byte) ((hex(field.charAt(i + 1)) << 4) | hex(field.charAt(i + 2)));
        i += 3;
      }
      try {
        decoded.append(StandardCharsets.UTF_8.newDecoder()//
            .onMalformedInput(CodingErrorAction.REPORT)//
            .onUnmappableCharacter(CodingErrorAction.REPORT)//
            .decode(ByteBuffer.wrap(bytes, 0, length)));
      } catch (final CharacterCodingException e) {
        throw new IllegalArgumentException("has a " + fieldName + " whose %-escapes are not valid UTF-8");
      }
    }
    return decoded.toString();
  }

  private static boolean isEscape(final String field, final int i) {
    return i + 2 < field.length() && field.charAt(i) == '%' && hex(field.charAt(i + 1)) >= 0 && hex(field.charAt(i + 2)) >= 0;
  }

  /** The value of an ASCII hexadecimal digit, -1 for anything else: Character.digit also accepts non-ASCII digits. */
  private static int hex(final char c) {
    if (c >= '0' && c <= '9')
      return c - '0';
    if (c >= 'a' && c <= 'f')
      return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
      return c - 'A' + 10;
    return -1;
  }
}
