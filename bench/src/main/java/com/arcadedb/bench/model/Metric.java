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
package com.arcadedb.bench.model;

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;

import java.util.Arrays;
import java.util.Locale;

/**
 * One measured quantity of a case: the median plus every repetition value, with the unit and the direction that counts as better.
 */
public record Metric(String name, String unit, Better better, double median, double[] values) {
  public enum Better {
    LOWER, HIGHER;

    public String toJSON() {
      return name().toLowerCase(Locale.ROOT);
    }

    public static Better fromJSON(final String value) {
      return valueOf(value.toUpperCase(Locale.ROOT));
    }
  }

  public JSONObject toJSON() {
    final JSONArray array = new JSONArray(values.length);
    for (final double v : values)
      array.put(v);
    return new JSONObject().put("name", name).put("unit", unit).put("better", better.toJSON()).put("median", median)
        .put("values", array);
  }

  public static Metric fromJSON(final JSONObject json) {
    final JSONArray array = json.getJSONArray("values", new JSONArray());
    final double[] values = new double[array.length()];
    for (int i = 0; i < values.length; i++)
      values[i] = array.getDouble(i);
    return new Metric(json.getString("name"), json.getString("unit"), Better.fromJSON(json.getString("better")),
        json.getDouble("median"), values);
  }

  @Override
  public boolean equals(final Object o) {
    return o instanceof final Metric m && name.equals(m.name) && unit.equals(m.unit) && better == m.better
        && Double.compare(median, m.median) == 0 && Arrays.equals(values, m.values);
  }

  @Override
  public int hashCode() {
    return 31 * (31 * (31 * name.hashCode() + unit.hashCode()) + better.hashCode() + Double.hashCode(median))
        + Arrays.hashCode(values);
  }

  @Override
  public String toString() {
    return "Metric[" + name + ", " + unit + ", " + better + ", " + median + ", " + Arrays.toString(values) + "]";
  }
}
