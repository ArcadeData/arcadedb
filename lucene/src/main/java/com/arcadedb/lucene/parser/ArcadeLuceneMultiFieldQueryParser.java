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
package com.arcadedb.lucene.parser;

import com.arcadedb.schema.Type;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.DateTools; // For date parsing, if needed
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.classic.MultiFieldQueryParser;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermRangeQuery; // For newStringRange
import org.apache.lucene.util.BytesRef;

import java.text.SimpleDateFormat; // Example for date parsing
import java.util.Date; // Example for date parsing
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ArcadeLuceneMultiFieldQueryParser extends MultiFieldQueryParser {

    private static final Logger logger = Logger.getLogger(ArcadeLuceneMultiFieldQueryParser.class.getName());

    private final Map<String, Type> fieldTypes;

    // Date format constants removed, will use LuceneDateTools

    public ArcadeLuceneMultiFieldQueryParser(Map<String, Type> fieldTypes, String[] fields, Analyzer analyzer, Map<String, Float> boosts) {
        super(fields, analyzer, boosts);
        this.fieldTypes = fieldTypes != null ? new HashMap<>(fieldTypes) : new HashMap<>();
    }

    public ArcadeLuceneMultiFieldQueryParser(Map<String, Type> fieldTypes, String[] fields, Analyzer analyzer) {
        super(fields, analyzer);
        this.fieldTypes = fieldTypes != null ? new HashMap<>(fieldTypes) : new HashMap<>();
    }

    protected Type getFieldType(String field) {
        return fieldTypes.get(field);
    }

    @Override
    protected Query newRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive) throws ParseException {
        Type fieldType = getFieldType(field);

        if (fieldType == null) {
            logger.log(Level.FINE, "No type information for field {0} in range query, defaulting to string range.", field);
            fieldType = Type.STRING; // Default to string range if type unknown
        }

        // Lucene's default MultiFieldQueryParser uses TermRangeQuery for ranges on text fields.
        // For specific data types, we need to create appropriate Point range queries.

        try {
            switch (fieldType) {
                case STRING:
                case TEXT:
                    // For string ranges, ensure part1 and part2 are not null for TermRangeQuery.newStringRange
                    // The superclass handles * as open range for TermRangeQuery.
                    // If super.newRangeQuery is called, it will likely create a TermRangeQuery.
                    // TermRangeQuery.newStringRange is more explicit for string ranges.
                    BytesRef lowerTerm = part1 == null ? null : new BytesRef(part1);
                    BytesRef upperTerm = part2 == null ? null : new BytesRef(part2);
                    return TermRangeQuery.newStringRange(field, part1, part2, startInclusive, endInclusive);

                case INTEGER:
                    Integer lowerInt = (part1 == null || "*".equals(part1)) ? null : Integer.parseInt(part1);
                    Integer upperInt = (part2 == null || "*".equals(part2)) ? null : Integer.parseInt(part2);
                    return IntPoint.newRangeQuery(field,
                            lowerInt == null ? Integer.MIN_VALUE : (startInclusive ? lowerInt : lowerInt + 1),
                            upperInt == null ? Integer.MAX_VALUE : (endInclusive ? upperInt : upperInt - 1));

                case LONG:
                case DATETIME:
                case DATE:
                    Long lowerLong = com.arcadedb.lucene.util.LuceneDateTools.parseDateTimeToMillis(part1);
                    Long upperLong = com.arcadedb.lucene.util.LuceneDateTools.parseDateTimeToMillis(part2);

                    if (fieldType == Type.DATE) {
                        if (lowerLong != null) lowerLong = com.arcadedb.lucene.util.LuceneDateTools.normalizeToDayEpochMillis(lowerLong);
                        if (upperLong != null) upperLong = com.arcadedb.lucene.util.LuceneDateTools.normalizeToDayEpochMillis(upperLong);
                    }

                    // Adjust for inclusive/exclusive after potential null from parsing
                    long actualLowerLong = lowerLong == null ? Long.MIN_VALUE : (startInclusive ? lowerLong : lowerLong + 1L);
                    if (lowerLong == null && "*".equals(part1)) actualLowerLong = Long.MIN_VALUE; // Explicit open start
                    else if (lowerLong == null && part1 != null) throw new ParseException("Cannot parse lower date range: " + part1);


                    long actualUpperLong = upperLong == null ? Long.MAX_VALUE : (endInclusive ? upperLong : upperLong - 1L);
                    if (upperLong == null && "*".equals(part2)) actualUpperLong = Long.MAX_VALUE; // Explicit open end
                    else if (upperLong == null && part2 != null) throw new ParseException("Cannot parse upper date range: " + part2);

                    // Ensure lower is not greater than upper after adjustments if both are specified
                    if (lowerLong != null && upperLong != null && actualLowerLong > actualUpperLong) {
                         actualLowerLong = lowerLong; // Reset to original parsed if adjustments inverted range for point fields
                         actualUpperLong = upperLong;
                         // For point fields, if startInclusive=false means actual_low = low+1, endInclusive=false means actual_high = high-1
                         // If after this actual_low > actual_high, it means no values can exist.
                         // Lucene's LongPoint.newRangeQuery handles this correctly by creating a query that matches nothing.
                    }

                    return LongPoint.newRangeQuery(field, actualLowerLong, actualUpperLong);
                case LONG: // Separate from DATE/DATETIME for clarity if parseDateTimeToMillis is too specific
                    Long lowerPlainLong = (part1 == null || "*".equals(part1)) ? null : Long.parseLong(part1);
                    Long upperPlainLong = (part2 == null || "*".equals(part2)) ? null : Long.parseLong(part2);
                    return LongPoint.newRangeQuery(field,
                            lowerPlainLong == null ? Long.MIN_VALUE : (startInclusive ? lowerPlainLong : lowerPlainLong + 1L),
                            upperPlainLong == null ? Long.MAX_VALUE : (endInclusive ? upperPlainLong : upperPlainLong - 1L));

                case FLOAT:
                    Float lowerFloat = (part1 == null || "*".equals(part1)) ? null : Float.parseFloat(part1);
                    Float upperFloat = (part2 == null || "*".equals(part2)) ? null : Float.parseFloat(part2);
                     // Point queries are exclusive for lower, inclusive for upper by default with null/MIN/MAX handling.
                     // Adjusting for inclusive/exclusive:
                    float actualLowerFloat = lowerFloat == null ? Float.NEGATIVE_INFINITY : (startInclusive ? lowerFloat : Math.nextUp(lowerFloat));
                    float actualUpperFloat = upperFloat == null ? Float.POSITIVE_INFINITY : (endInclusive ? upperFloat : Math.nextDown(upperFloat));
                    return FloatPoint.newRangeQuery(field, actualLowerFloat, actualUpperFloat);


                case DOUBLE:
                    Double lowerDouble = (part1 == null || "*".equals(part1)) ? null : Double.parseDouble(part1);
                    Double upperDouble = (part2 == null || "*".equals(part2)) ? null : Double.parseDouble(part2);
                    double actualLowerDouble = lowerDouble == null ? Double.NEGATIVE_INFINITY : (startInclusive ? lowerDouble : Math.nextUp(lowerDouble));
                    double actualUpperDouble = upperDouble == null ? Double.POSITIVE_INFINITY : (endInclusive ? upperDouble : Math.nextDown(upperDouble));
                    return DoublePoint.newRangeQuery(field, actualLowerDouble, actualUpperDouble);

                case SHORT:
                case BYTE:
                     // Promote to IntPoint for querying, as Lucene has no ShortPoint/BytePoint
                    Integer lowerShortOrByte = (part1 == null || "*".equals(part1)) ? null : Integer.parseInt(part1);
                    Integer upperShortOrByte = (part2 == null || "*".equals(part2)) ? null : Integer.parseInt(part2);
                    return IntPoint.newRangeQuery(field,
                            lowerShortOrByte == null ? Integer.MIN_VALUE : (startInclusive ? lowerShortOrByte : lowerShortOrByte + 1),
                            upperShortOrByte == null ? Integer.MAX_VALUE : (endInclusive ? upperShortOrByte : upperShortOrByte - 1));

                default:
                    logger.log(Level.WARNING, "Unhandled type {0} for field {1} in range query. Defaulting to string range.", new Object[]{fieldType, field});
                    return TermRangeQuery.newStringRange(field, part1, part2, startInclusive, endInclusive);
            }
        } catch (NumberFormatException e) {
            throw new ParseException("Could not parse number in range query for field " + field + ": " + e.getMessage());
        }
        // Removed catch for java.text.ParseException as LuceneDateTools handles its own parsing issues or returns null
    }

    // Date parsing helper removed, now using LuceneDateTools

    // Wildcard, Prefix, Fuzzy queries usually apply to text fields.
    // The superclass versions are generally fine. If specific behavior is needed
    // for non-text fields (e.g., to disallow or handle differently),
    // these methods can be overridden. For now, relying on superclass.

    // @Override
    // protected Query getWildcardQuery(String field, String termStr) throws ParseException {
    //     Type fieldType = getFieldType(field);
    //     if (fieldType != null && fieldType.isNumeric()) {
    //         // Wildcards on numeric points don't make sense.
    //         // Could throw error or return a MatchNoDocsQuery, or let super handle (might error).
    //         // For now, let super decide, it might try to parse termStr as a number.
    //     }
    //     return super.getWildcardQuery(field, termStr);
    // }
}
