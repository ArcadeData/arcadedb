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

import com.arcadedb.utility.RegexAutomaton;
import org.junit.jupiter.api.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9853: a REGEXP on a number or a temporal is refused when no text of the type's form matches it, so the form must
 * describe EVERY text a value of the class is written as. A value whose text falls outside it would have a pattern it
 * satisfies refused at declaration time. This pins the form against the edge values of every class, which the forms carry as
 * their samples (issue #9990), and pins every shape as one the automaton the check decides with compiles.
 */
class Issue9853TextFormCoversEveryValueTest {

  @Test
  void everyValueIsWrittenInsideTheFormOfItsClass() {
    for (final TextForm form : TextForm.all()) {
      assertThat(form.samples()).isNotEmpty();
      for (final Object value : form.samples()) {
        assertThat(TextForm.of(value.getClass())).as(value.getClass().getSimpleName()).isSameAs(form);
        assertCovered(value);
      }
    }

    // java.util.Date writes the JVM's default zone, by its short name, and so does a Calendar
    final TimeZone defaultZone = TimeZone.getDefault();
    try {
      for (final String zone : List.of("Asia/Kolkata", "Pacific/Guam", "America/St_Johns", "Australia/Lord_Howe", "GMT",
          "Etc/GMT+5")) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        assertCovered(new Date(1_700_000_000_000L));
        assertCovered(Calendar.getInstance());
      }
    } finally {
      TimeZone.setDefault(defaultZone);
    }
  }

  @Test
  void everyShapeIsDecidedByAnAutomaton() {
    for (final TextForm form : TextForm.all()) {
      final RegexAutomaton shape = RegexAutomaton.compile(form.shape().pattern(), form.characters());
      assertThat(shape).as(form.shape().pattern()).isNotNull();
      for (final Object value : form.samples())
        assertThat(shape.matches(value.toString())).as("'%s' of the shape '%s'", value, form.shape().pattern()).isTrue();
    }
  }

  private static void assertCovered(final Object value) {
    final TextForm form = TextForm.of(value.getClass());
    final String text = value.toString();
    assertThat(form).as(value.getClass().getSimpleName()).isNotNull();
    assertThat(form.shape().matcher(text).matches()).as("'%s' of the shape '%s'", text, form.shape().pattern()).isTrue();
    for (int i = 0; i < text.length(); i++)
      assertThat(form.characters()).as("'%s' written with '%s'", text, form.characters()).contains(String.valueOf(text.charAt(i)));
  }
}
