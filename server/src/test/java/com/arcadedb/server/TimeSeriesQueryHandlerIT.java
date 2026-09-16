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

import com.arcadedb.serializer.json.JSONArray;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for TimeSeries query and latest HTTP endpoints.
 */
class TimeSeriesQueryHandlerIT extends BaseGraphServerTest {

  @Test
  void rawQuery() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");
      request.put("from", 1000L);
      request.put("to", 3000L);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();
      assertThat(result.getString("type")).isEqualTo("weather");
      assertThat(result.getInt("count")).isEqualTo(3);

      final JSONArray columns = result.getJSONArray("columns");
      assertThat(columns.length()).isGreaterThanOrEqualTo(3);

      final JSONArray rows = result.getJSONArray("rows");
      assertThat(rows.length()).isEqualTo(3);
    });
  }

  /**
   * Issue #5711: a response the row limit cut short must say so instead of looking like a complete one.
   */
  @Test
  void rawQueryReportsTruncation() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");
      request.put("from", 1000L);
      request.put("to", 3000L);
      request.put("limit", 2);

      final JSONObject truncated = postTsQuery(serverIndex, request);
      assertThat(truncated.getInt("count")).isEqualTo(2);
      assertThat(truncated.getInt("limit")).isEqualTo(2);
      assertThat(truncated.getBoolean("truncated")).isTrue();

      request.put("limit", 3);
      final JSONObject complete = postTsQuery(serverIndex, request);
      assertThat(complete.getInt("count")).isEqualTo(3);
      assertThat(complete.getBoolean("truncated")).isFalse();

      // A non-positive limit means unlimited here too, as it does on the query/command endpoints: it used to
      // reach Math.min(rows, -1) and return no row at all while reporting the result as truncated.
      // A limit an int cannot hold must not wrap into a negative value and be read as unlimited: it is a client
      // error here exactly as it is on the query and command endpoints.
      request.put("limit", 3_000_000_000L);
      assertThat(postTsQueryRaw(serverIndex, request)).isEqualTo(400);

      for (final int unlimitedLimit : new int[] { -1, 0 }) {
        request.put("limit", unlimitedLimit);
        final JSONObject unlimited = postTsQuery(serverIndex, request);
        assertThat(unlimited.getInt("count")).isEqualTo(3);
        assertThat(unlimited.getInt("limit")).isEqualTo(-1);
        assertThat(unlimited.getBoolean("truncated")).isFalse();
      }
    });
  }

  @Test
  void aggregatedQuery() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", 5000L);

      final JSONArray requests = new JSONArray();
      final JSONObject avgReq = new JSONObject();
      avgReq.put("field", "temperature");
      avgReq.put("type", "AVG");
      avgReq.put("alias", "avg_temp");
      requests.put(avgReq);
      aggregation.put("requests", requests);

      request.put("aggregation", aggregation);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();
      assertThat(result.getString("type")).isEqualTo("weather");
      assertThat(result.getInt("count")).isGreaterThan(0);

      final JSONArray aggregations = result.getJSONArray("aggregations");
      assertThat(aggregations.getString(0)).isEqualTo("avg_temp");

      final JSONArray buckets = result.getJSONArray("buckets");
      assertThat(buckets.length()).isGreaterThan(0);

      final JSONObject firstBucket = buckets.getJSONObject(0);
      assertThat(firstBucket.has("timestamp")).isTrue();
      assertThat(firstBucket.has("values")).isTrue();
    });
  }

  /**
   * Issue #7325: an aggregation function the server does not know must be refused with a message that names the
   * field and lists the accepted values. It used to reach {@code AggregationType.valueOf} unguarded, whose
   * {@code IllegalArgumentException} the generic mapper answers as "Cannot execute command" with the specifics in
   * the {@code detail} field - and {@code detail} is concealed whenever the server runs in production mode, so the
   * caller was left with no way to tell which field was wrong.
   */
  @Test
  void aggregationRejectsUnknownTypeWithANamedError() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject error = postTsQueryError(serverIndex, aggregationRequest("MEDIAN"));

      assertThat(error.getString("error"))
          .as("must name the field that was wrong and every value it accepts")
          .contains("aggregation.requests[0].type")
          .contains("SUM", "AVG", "MIN", "MAX", "COUNT")
          .contains("MEDIAN");
    });
  }

  /**
   * Issue #7325: a missing aggregation function is the same client error as an unknown one and gets the same
   * answer, instead of the JSONException the raw {@code getString} used to throw.
   */
  @Test
  void aggregationTypeIsRequired() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject error = postTsQueryError(serverIndex, aggregationRequest(null));

      assertThat(error.getString("error"))
          .contains("aggregation.requests[0].type")
          .contains("SUM", "AVG", "MIN", "MAX", "COUNT");
    });
  }

  /**
   * Issue #7325: the lower-cased spelling a hand-written client is most likely to send now resolves instead of
   * being refused, and the default alias is unchanged by which spelling arrived.
   */
  @Test
  void aggregationTypeIsCaseInsensitive() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      for (final String spelling : new String[] { "avg", " Avg ", "AVG" }) {
        final JSONObject result = postTsQuery(serverIndex, aggregationRequest(spelling));

        assertThat(result.getJSONArray("aggregations").getString(0))
            .as("alias defaults to the field name plus the canonical lower-cased function name")
            .isEqualTo("temperature_avg");
        assertThat(result.getJSONArray("buckets").length()).isGreaterThan(0);
      }
    });
  }

  /**
   * Issue #7340: every OTHER required member of an aggregation request is refused the way #7325 made {@code type}
   * be refused - a message that names the member by its request path, carried in the {@code error} field.
   * <p>
   * They used to reach {@code JSONObject}'s raising getters unguarded, whose {@code JSONException} the generic
   * handler mapper answers as {@code 400 "Invalid JSON payload"} with the specifics in {@code detail} - and
   * {@code buildErrorBody} CONCEALS {@code detail} whenever the server runs in production mode. A caller that
   * omitted {@code field} therefore received a body that did not say which member was missing.
   */
  @Test
  void aggregationRefusesEveryAbsentRequiredMemberByName() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      // 'aggregation.bucketInterval' absent.
      JSONObject request = aggregationRequest("AVG");
      request.getJSONObject("aggregation").remove("bucketInterval");
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation.bucketInterval").contains("is required");

      // 'aggregation.requests' absent.
      request = aggregationRequest("AVG");
      request.getJSONObject("aggregation").remove("requests");
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation.requests").contains("is required");

      // 'aggregation.requests[0].field' absent.
      request = aggregationRequest("AVG");
      request.getJSONObject("aggregation").getJSONArray("requests").getJSONObject(0).remove("field");
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation.requests[0].field").contains("is required");
    });
  }

  /**
   * Issue #7340: the same members are refused by name when they ARRIVE but carry the wrong JSON type, which is the
   * other half of what {@code getNotNullElement} and the converters used to signal as a bare {@code JSONException}.
   */
  @Test
  void aggregationRefusesAWronglyTypedRequiredMemberByName() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      // 'aggregation' itself is not an object.
      JSONObject request = new JSONObject();
      request.put("type", "weather");
      request.put("aggregation", "AVG");
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation").contains("must be a JSON object");

      // 'aggregation.requests' is not an array.
      request = aggregationRequest("AVG");
      request.getJSONObject("aggregation").put("requests", new JSONObject());
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation.requests").contains("must be a JSON array");

      // 'aggregation.requests[0]' is not an object.
      request = aggregationRequest("AVG");
      final JSONArray notObjects = new JSONArray();
      notObjects.put("AVG");
      request.getJSONObject("aggregation").put("requests", notObjects);
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation.requests[0]").contains("must be a JSON object");

      // 'aggregation.bucketInterval' is not a number.
      request = aggregationRequest("AVG");
      request.getJSONObject("aggregation").put("bucketInterval", new JSONArray());
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("aggregation.bucketInterval").contains("must be a number");
    });
  }

  /**
   * claude-review on PR #7680: the refusals that quote the caller's own text back at it have to stay PARSEABLE
   * when that text contains a double quote. Two of them were still hand-built strings, so a type named
   * {@code we"ather} produced a body no client can read - which on an error path is worse than the error, because
   * the caller cannot even see what went wrong.
   * <p>
   * {@code postTsQueryError} parses the body with {@code new JSONObject(...)}, so an unparseable body fails this
   * test before any assertion on its content runs.
   */
  @Test
  void refusalsStayParseableWhenTheCallersTextCarriesADoubleQuote() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      // 'Type ... does not exist' - was concatenated.
      final JSONObject unknownType = new JSONObject();
      unknownType.put("type", "we\"ather");
      assertThat(postTsQueryError(serverIndex, unknownType).getString("error"))
          .contains("we\"ather").contains("does not exist");

      // 'Type ... is not a TimeSeries type' - was concatenated.
      command(serverIndex, "CREATE DOCUMENT TYPE `notts7680`");
      final JSONObject notTs = new JSONObject();
      notTs.put("type", "notts7680");
      assertThat(postTsQueryError(serverIndex, notTs).getString("error"))
          .contains("notts7680").contains("is not a TimeSeries type");

      // 'Field ... not found in type' - was concatenated.
      final JSONObject aggRequest = new JSONObject();
      aggRequest.put("field", "tem\"perature");
      aggRequest.put("type", "AVG");

      final JSONArray requests = new JSONArray();
      requests.put(aggRequest);

      final JSONObject aggregation = new JSONObject();
      aggregation.put("bucketInterval", 5000L);
      aggregation.put("requests", requests);

      final JSONObject badField = new JSONObject();
      badField.put("type", "weather");
      badField.put("aggregation", aggregation);

      assertThat(postTsQueryError(serverIndex, badField).getString("error"))
          .contains("tem\"perature").contains("not found in type");
    });
  }

  /**
   * Issue #7340: the members the raw branch reads are refused by name too. {@code type} had a presence guard but
   * not a type guard, and {@code tags}/{@code fields}/{@code limit} had neither - all four answered through the
   * concealed {@code detail} field.
   */
  @Test
  void rawQueryRefusesAWronglyTypedMemberByName() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      // An absent 'type' and a wrongly-typed one are the same client error and now get the SAME sentence: the
      // handler used to carry a second, differently worded pre-guard for the absent case only.
      assertThat(postTsQueryError(serverIndex, new JSONObject()).getString("error"))
          .isEqualTo("'type' is required and must be a string");

      JSONObject request = new JSONObject();
      request.put("type", new JSONArray());
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("'type'").contains("must be a string");

      request = new JSONObject();
      request.put("type", "weather");
      request.put("tags", new JSONArray());
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("'tags'").contains("must be a JSON object");

      request = new JSONObject();
      request.put("type", "weather");
      request.put("fields", new JSONObject());
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("'fields'").contains("must be a JSON array");

      request = new JSONObject();
      request.put("type", "weather");
      final JSONArray notStrings = new JSONArray();
      notStrings.put(new JSONObject());
      request.put("fields", notStrings);
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("fields[0]").contains("must be a string");

      request = new JSONObject();
      request.put("type", "weather");
      request.put("from", "yesterday");
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("'from'").contains("must be a number");

      request = new JSONObject();
      request.put("type", "weather");
      request.put("limit", "ten");
      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .as("the limit refusal has to be readable too, not hidden in the concealed 'detail' field")
          .contains("limit");

      // An explicitly null optional member states that the caller did not supply it, the same reading 'tags' and
      // 'fields' get - not a malformed request.
      request = new JSONObject();
      request.put("type", "weather");
      request.put("from", (Object) null);
      request.put("tags", (Object) null);
      request.put("fields", (Object) null);
      request.put("aggregation", (Object) null);
      assertThat(postTsQuery(serverIndex, request).getInt("count")).isEqualTo(3);
    });
  }

  /**
   * Builds a bucketed aggregation over the ingested "weather" type whose single request carries the given
   * function name, or no "type" member at all when it is null.
   */
  private static JSONObject aggregationRequest(final String aggregationType) {
    final JSONObject aggRequest = new JSONObject();
    aggRequest.put("field", "temperature");
    if (aggregationType != null)
      aggRequest.put("type", aggregationType);

    final JSONArray requests = new JSONArray();
    requests.put(aggRequest);

    final JSONObject aggregation = new JSONObject();
    aggregation.put("bucketInterval", 5000L);
    aggregation.put("requests", requests);

    final JSONObject request = new JSONObject();
    request.put("type", "weather");
    request.put("aggregation", aggregation);
    return request;
  }

  @Test
  void queryWithTagFilter() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");

      final JSONObject tags = new JSONObject();
      tags.put("location", "us-east");
      request.put("tags", tags);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();

      final JSONArray rows = result.getJSONArray("rows");
      assertThat(rows.length()).isEqualTo(2);
    });
  }

  @Test
  void queryWithFieldProjection() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "weather");

      final JSONArray fields = new JSONArray();
      fields.put("temperature");
      request.put("fields", fields);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result).isNotNull();

      final JSONArray columns = result.getJSONArray("columns");
      // Should have timestamp + temperature only
      assertThat(columns.length()).isEqualTo(2);
      assertThat(columns.getString(0)).isEqualTo("ts");
      assertThat(columns.getString(1)).isEqualTo("temperature");
    });
  }

  @Test
  void queryMissingType() throws Exception {
    testEachServer(serverIndex -> {
      final JSONObject request = new JSONObject();
      // No "type" field

      final int statusCode = postTsQueryRaw(serverIndex, request);
      assertThat(statusCode).isEqualTo(400);
    });
  }

  @Test
  void queryNonTimeSeriesType() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex, "CREATE DOCUMENT TYPE notts");

      final JSONObject request = new JSONObject();
      request.put("type", "notts");

      final int statusCode = postTsQueryRaw(serverIndex, request);
      assertThat(statusCode).isEqualTo(400);
    });
  }

  /**
   * Regression for issue #6356's follow-up (claude-review on PR #6779): a TimeSeries type whose engine failed to
   * load must be told apart from "not a TimeSeries type" here too, on the query endpoint.
   */
  @Test
  void queryEngineUnavailableTypeIsReportedDistinctlyFromNonTimeSeriesType() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE broken TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)");
      command(serverIndex, "INSERT INTO broken SET ts = 1700000000000, host = 'h', usage = 1.0");

      corruptSealedStoreAndReopen(serverIndex, "broken");

      final JSONObject request = new JSONObject();
      request.put("type", "broken");

      final JSONObject error = postTsQueryError(serverIndex, request);
      assertThat(error.getString("error")).as("must be distinguishable from \"is not a TimeSeries type\"")
          .contains("no storage engine available");
    });
  }

  @Test
  void latestValue() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject result = getTsLatest(serverIndex, "weather", null);
      assertThat(result).isNotNull();
      assertThat(result.getString("type")).isEqualTo("weather");

      final JSONArray latest = result.getJSONArray("latest");
      assertThat(latest).isNotNull();
      // Latest timestamp should be 3000
      assertThat(latest.getLong(0)).isEqualTo(3000L);
    });
  }

  @Test
  void latestWithTagFilter() throws Exception {
    testEachServer(serverIndex -> {
      createTypeAndIngestData(serverIndex);

      final JSONObject result = getTsLatest(serverIndex, "weather", "location:us-west");
      assertThat(result).isNotNull();

      final JSONArray latest = result.getJSONArray("latest");
      assertThat(latest).isNotNull();
      // us-west has only one entry at timestamp 2000
      assertThat(latest.getLong(0)).isEqualTo(2000L);
    });
  }

  /**
   * Issue #7321: 'latest' honoured only the first 'tag' occurrence, so on a type with more than one tag
   * column it could not name one series. Every occurrence now contributes an ANDed condition, the way
   * POST /ts/{database}/query has always conjoined its 'tags' object.
   * <p>
   * The fixture is built so that no single tag can produce the expected answer on its own: host=web1 alone
   * is newest at 4000 in region=us, and region=eu alone is newest at 4000 on host=web2. Only the
   * conjunction host=web1 AND region=eu selects the sample at 3000, so a filter that dropped either
   * occurrence would fail this assertion rather than pass it by coincidence.
   */
  @Test
  void latestNarrowsOnEveryRepeatedTagOccurrence() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      final JSONObject result = getTsLatestWithTags(serverIndex, "machines", "host:web1", "region:eu");
      assertThat(result).isNotNull();

      final JSONArray latest = result.getJSONArray("latest");
      assertThat(latest).as("host=web1 AND region=eu holds exactly one sample, at 3000").isNotNull();
      assertThat(latest.getLong(0)).isEqualTo(3000L);

      // Order of the occurrences must not matter: the conditions are ANDed, not positional.
      final JSONObject reversed = getTsLatestWithTags(serverIndex, "machines", "region:eu", "host:web1");
      assertThat(reversed.getJSONArray("latest").getLong(0)).isEqualTo(3000L);
    });
  }

  /**
   * Issue #7321: proves the two occurrences above are genuinely both applied, by showing what each one
   * yields on its own. Either single tag lands on 4000, so the 3000 the pair returns can only come from
   * the conjunction.
   */
  @Test
  void latestWithASingleTagIsUnchangedByTheRepeatableParameter() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      assertThat(getTsLatestWithTags(serverIndex, "machines", "host:web1").getJSONArray("latest").getLong(0))
          .as("host=web1 alone is newest at 4000, in region=us").isEqualTo(4000L);
      assertThat(getTsLatestWithTags(serverIndex, "machines", "region:eu").getJSONArray("latest").getLong(0))
          .as("region=eu alone is newest at 4000, on host=web2").isEqualTo(4000L);
    });
  }

  /**
   * Issue #7334: an occurrence that resolves to no TAG column is refused with a 400 naming it and listing the
   * type's declared tags, not silently dropped.
   * <p>
   * On this endpoint dropping it was worse than anywhere else, because {@code latest} answers ONE row: the
   * caller got the newest sample of some other series as if it were the one they asked for. The refusal has to
   * carry its message in {@code error} rather than in {@code detail}, which the handler mapper conceals outside
   * development mode - a concealed reason would leave the caller with a bare 400 and nothing to fix.
   */
  @Test
  void latestRefusesAnOccurrenceThatNamesNoTagColumn() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      final JSONObject error = getTsLatestError(serverIndex, "machines", "host:web1", "region:eu", "nosuchtag:x");
      assertThat(error.getString("error"))
          .contains("nosuchtag")
          .as("the message lists what the type DOES declare, because the name is almost always a misspelling")
          .contains("host")
          .contains("region");

      // The correct spelling of the same request still answers, so this is a refusal and not a breakage.
      assertThat(getTsLatestWithTags(serverIndex, "machines", "host:web1", "region:eu")
          .getJSONArray("latest").getLong(0)).isEqualTo(3000L);
    });
  }

  /** Issue #7334: an occurrence that is not in name:value form is refused for the same reason. */
  @Test
  void latestRefusesAnOccurrenceWithNoSeparator() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      assertThat(getTsLatestError(serverIndex, "machines", "host").getString("error"))
          .contains("name:value");
    });
  }

  /**
   * Issue #7334: the {@code tags} object of POST /ts/{database}/query is refused the same way. This is the
   * widening case - a mistyped name used to leave a conjunction one term short, so the query returned every row
   * of the range, indistinguishable from a filter that legitimately matched everything.
   */
  @Test
  void queryRefusesATagNameThatMatchesNoTagColumn() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "machines");
      request.put("tags", new JSONObject().put("host", "web1").put("hsot", "web1"));

      assertThat(postTsQueryError(serverIndex, request).getString("error"))
          .contains("hsot")
          .contains("host");
    });
  }

  /**
   * Issue #7321: occurrences are ANDed, so two that name the same tag with different values ask for a
   * sample that is both and select nothing. Pinned because the alternative reading - treating repeats of
   * one name as a set membership test - is a plausible thing for someone to "fix" this into later, and it
   * would silently widen every such request instead of emptying it.
   */
  @Test
  void latestAndsTwoOccurrencesOfTheSameTagRatherThanUnioningThem() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      final JSONObject result = getTsLatestWithTags(serverIndex, "machines", "host:web1", "host:web2");
      assertThat(result.isNull("latest")).as("no sample has host both web1 and web2").isTrue();
    });
  }

  /**
   * Issue #7321 guards the refactor that moved the name-to-column resolution into a helper shared with
   * this endpoint: POST /ts/{database}/query must still AND every pair of its 'tags' object.
   */
  @Test
  void queryConjoinsEveryTagPair() throws Exception {
    testEachServer(serverIndex -> {
      createMultiTagTypeAndIngestData(serverIndex);

      final JSONObject request = new JSONObject();
      request.put("type", "machines");
      final JSONObject tags = new JSONObject();
      tags.put("host", "web1");
      tags.put("region", "eu");
      request.put("tags", tags);

      final JSONObject result = postTsQuery(serverIndex, request);
      assertThat(result.getInt("count")).as("only the sample at 3000 carries both tags").isEqualTo(1);
      assertThat(result.getJSONArray("rows").getJSONArray(0).getLong(0)).isEqualTo(3000L);
    });
  }

  @Test
  void latestEmptyType() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE emptyts TIMESTAMP ts TAGS (tag1 STRING) FIELDS (value DOUBLE)");

      final JSONObject result = getTsLatest(serverIndex, "emptyts", null);
      assertThat(result).isNotNull();
      assertThat(result.isNull("latest")).isTrue();
    });
  }

  @Test
  void latestMissingType() throws Exception {
    testEachServer(serverIndex -> {
      final int statusCode = getTsLatestRaw(serverIndex, null, null);
      assertThat(statusCode).isEqualTo(400);
    });
  }

  /**
   * Regression for issue #6356's follow-up (claude-review on PR #6779): a TimeSeries type whose engine failed to
   * load must be told apart from "not a TimeSeries type" here too, on the latest-value endpoint.
   */
  @Test
  void latestEngineUnavailableTypeIsReportedDistinctlyFromNonTimeSeriesType() throws Exception {
    testEachServer(serverIndex -> {
      command(serverIndex,
          "CREATE TIMESERIES TYPE broken TIMESTAMP ts TAGS (host STRING) FIELDS (usage DOUBLE)");
      command(serverIndex, "INSERT INTO broken SET ts = 1700000000000, host = 'h', usage = 1.0");

      corruptSealedStoreAndReopen(serverIndex, "broken");

      final JSONObject error = getTsLatestError(serverIndex, "broken");
      assertThat(error.getString("error")).as("must be distinguishable from \"is not a TimeSeries type\"")
          .contains("no storage engine available");
    });
  }

  private void createTypeAndIngestData(final int serverIndex) throws Exception {
    command(serverIndex,
        "CREATE TIMESERIES TYPE weather TIMESTAMP ts TAGS (location STRING) FIELDS (temperature DOUBLE)");

    final String lineProtocol = """
        weather,location=us-east temperature=22.5 1000
        weather,location=us-west temperature=18.3 2000
        weather,location=us-east temperature=23.1 3000
        """;

    final int statusCode = postLineProtocol(serverIndex, lineProtocol, "ms");
    assertThat(statusCode).isEqualTo(204);
  }

  /**
   * A type with two tag columns, the shape issue #7321 is about. No single tag identifies the sample at
   * 3000: host=web1 is also present at 4000 and region=eu is also present at 4000.
   */
  private void createMultiTagTypeAndIngestData(final int serverIndex) throws Exception {
    command(serverIndex,
        "CREATE TIMESERIES TYPE machines TIMESTAMP ts TAGS (host STRING, region STRING) FIELDS (cpu DOUBLE)");

    final String lineProtocol = """
        machines,host=web1,region=us cpu=10.0 1000
        machines,host=web2,region=eu cpu=20.0 2000
        machines,host=web1,region=eu cpu=30.0 3000
        machines,host=web1,region=us cpu=40.0 4000
        machines,host=web2,region=eu cpu=50.0 4000
        """;

    assertThat(postLineProtocol(serverIndex, lineProtocol, "ms")).isEqualTo(204);
  }

  private int postLineProtocol(final int serverIndex, final String body, final String precision) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/write?precision=" + precision)
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "text/plain");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(body.getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  private JSONObject postTsQuery(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/query")
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private int postTsQueryRaw(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/query")
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    return connection.getResponseCode();
  }

  private JSONObject postTsQueryError(final int serverIndex, final JSONObject request) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/query")
        .toURL()
        .openConnection();

    connection.setRequestMethod("POST");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));
    connection.setRequestProperty("Content-Type", "application/json");
    connection.setDoOutput(true);

    try (final OutputStream os = connection.getOutputStream()) {
      os.write(request.toString().getBytes(StandardCharsets.UTF_8));
      os.flush();
    }

    assertThat(connection.getResponseCode()).isEqualTo(400);
    return new JSONObject(readError(connection));
  }

  /**
   * Issue #7334: the 400 body of a refused {@code tag} occurrence. Separate from
   * {@link #getTsLatestWithTags} because that one asserts a 200 - a refusal has to be read from the error
   * stream, and the message has to be in {@code error} rather than the concealed {@code detail}.
   */
  private JSONObject getTsLatestError(final int serverIndex, final String type, final String... tags)
      throws Exception {
    final StringBuilder url = new StringBuilder(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest?type=" + type);
    for (final String tag : tags)
      url.append("&tag=").append(URLEncoder.encode(tag, StandardCharsets.UTF_8));

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString()).toURL().openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    assertThat(connection.getResponseCode()).isEqualTo(400);
    return new JSONObject(readError(connection));
  }

  /**
   * Issue #7321: sends one 'tag' query parameter per entry, which is what a repeated parameter looks like
   * on the wire. Values are percent-encoded because a tag value is caller text and may carry a ':' of its
   * own past the first separator.
   */
  private JSONObject getTsLatestWithTags(final int serverIndex, final String type, final String... tags)
      throws Exception {
    final StringBuilder url = new StringBuilder(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest?type=" + type);
    for (final String tag : tags)
      url.append("&tag=").append(URLEncoder.encode(tag, StandardCharsets.UTF_8));

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString()).toURL().openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private JSONObject getTsLatest(final int serverIndex, final String type, final String tag) throws Exception {
    final StringBuilder url = new StringBuilder("http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest?type=" + type);
    if (tag != null)
      url.append("&tag=").append(tag);

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString())
        .toURL()
        .openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    assertThat(connection.getResponseCode()).isEqualTo(200);

    try (final InputStream is = connection.getInputStream()) {
      return new JSONObject(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  private int getTsLatestRaw(final int serverIndex, final String type, final String tag) throws Exception {
    final StringBuilder url = new StringBuilder("http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest");
    if (type != null)
      url.append("?type=").append(type);
    if (tag != null)
      url.append(type != null ? "&" : "?").append("tag=").append(tag);

    final HttpURLConnection connection = (HttpURLConnection) new URI(url.toString())
        .toURL()
        .openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    return connection.getResponseCode();
  }

  private JSONObject getTsLatestError(final int serverIndex, final String type) throws Exception {
    final HttpURLConnection connection = (HttpURLConnection) new URI(
        "http://127.0.0.1:248" + serverIndex + "/api/v1/ts/graph/latest?type=" + type)
        .toURL()
        .openConnection();

    connection.setRequestMethod("GET");
    connection.setRequestProperty("Authorization",
        "Basic " + Base64.getEncoder().encodeToString(("root:" + BaseGraphServerTest.DEFAULT_PASSWORD_FOR_TESTS).getBytes()));

    assertThat(connection.getResponseCode()).isEqualTo(400);
    return new JSONObject(readError(connection));
  }
}
