/*
 * Copyright 2023 Arcade Data Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcadedb.lucene.util;

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LuceneDateTools {

    private static final Logger logger = Logger.getLogger(LuceneDateTools.class.getName());

    // Prioritized list of date/datetime formatters
    // ISO 8601 with Z / offset / local
    private static final DateTimeFormatter ISO_OFFSET_DATE_TIME = DateTimeFormatter.ISO_OFFSET_DATE_TIME; // Handles 'Z' and offsets like +01:00
    private static final DateTimeFormatter ISO_LOCAL_DATE_TIME = DateTimeFormatter.ISO_LOCAL_DATE_TIME;   // Handles 'yyyy-MM-ddTHH:mm:ss.SSS'
    private static final DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE;         // Handles 'yyyy-MM-dd'

    // Common alternative formats
    private static final String ALT_DATETIME_FORMAT_NO_T = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String ALT_DATETIME_FORMAT_NO_T_NO_MS = "yyyy-MM-dd HH:mm:ss";
    private static final String ALT_DATETIME_FORMAT_NO_T_NO_S_NO_MS = "yyyy-MM-dd HH:mm";


    public static Long parseDateTimeToMillis(String dateTimeString) {
        if (dateTimeString == null || dateTimeString.isEmpty() || "*".equals(dateTimeString)) {
            return null;
        }

        // 1. Try parsing as plain long (epoch millis)
        try {
            return Long.parseLong(dateTimeString);
        } catch (NumberFormatException e) {
            // Not a long, proceed to date formats
        }

        // 2. Try ISO_OFFSET_DATE_TIME (handles 'Z' for UTC and offsets)
        try {
            OffsetDateTime odt = OffsetDateTime.parse(dateTimeString, ISO_OFFSET_DATE_TIME);
            return odt.toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            // ignore and try next format
        }

        // 3. Try ISO_LOCAL_DATE_TIME (assumes system default timezone if no offset specified)
        // To be safer, we should assume UTC if no offset is present, or make it configurable.
        // For now, let's try parsing as local and then converting to UTC for consistency.
        try {
            LocalDateTime ldt = LocalDateTime.parse(dateTimeString, ISO_LOCAL_DATE_TIME);
            return ldt.toInstant(ZoneOffset.UTC).toEpochMilli(); // Assume UTC if no offset
        } catch (DateTimeParseException e) {
            // ignore and try next format
        }

        // 4. Try ISO_LOCAL_DATE (assumes start of day, UTC)
        try {
            LocalDate ld = LocalDate.parse(dateTimeString, ISO_LOCAL_DATE);
            return ld.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
        } catch (DateTimeParseException e) {
            // ignore and try next format
        }

        // 5. Try alternative SimpleDateFormat patterns (less robust, more ambiguous)
        // These assume UTC. If local timezone is implied by strings, SimpleDateFormat needs setTimeZone(TimeZone.getDefault())
        // but for consistency with Lucene (which often uses UTC via DateTools), UTC is safer.
        String[] altPatterns = {
                ALT_DATETIME_FORMAT_NO_T,
                ALT_DATETIME_FORMAT_NO_T_NO_MS,
                ALT_DATETIME_FORMAT_NO_T_NO_S_NO_MS
        };

        for (String pattern : altPatterns) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat(pattern);
                sdf.setTimeZone(TimeZone.getTimeZone("UTC")); // Assume UTC for these patterns too
                sdf.setLenient(false);
                Date date = sdf.parse(dateTimeString);
                return date.getTime();
            } catch (java.text.ParseException ex) {
                // ignore and try next pattern
            }
        }

        logger.log(Level.WARNING, "Failed to parse date/datetime string: {0}", dateTimeString);
        return null; // Or throw ParseException if strict parsing is required
    }

    public static Long normalizeToDayEpochMillis(long epochMillis) {
        Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.setTimeInMillis(epochMillis);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }

     public static Long parseDateToMillis(String dateString) {
        Long epochMillis = parseDateTimeToMillis(dateString);
        if (epochMillis != null) {
            return normalizeToDayEpochMillis(epochMillis);
        }
        return null;
    }
}
