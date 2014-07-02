/*
 * Copyright 2014 Higher Frequency Trading
 * <p/>
 * http://www.higherfrequencytrading.com
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.collections;

import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author Rob Austin.
 */
public abstract class ExternalReplicatorBuilder<V, B extends ExternalReplicatorBuilder> {
    final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormat.forPattern("YYYY-MM-dd HH:mm:ss.S")
            .withZoneUTC();

    public static final String YYYY_MM_DD = "YYYY-MM-dd";

    private DateTimeZone dateTimeZone = DEFAULT_DATE_TIME_FORMATTER.getZone();
    private DateTimeFormatter dateTimeFormatter = DEFAULT_DATE_TIME_FORMATTER;
    private DateTimeFormatter shortDateTimeFormatter = DateTimeFormat.forPattern(YYYY_MM_DD).withZone(dateTimeFormatter.getZone());
    private String shortDateTimeFormatterStr = YYYY_MM_DD;
    private ExternalReplicator.FieldMapper<V> fieldMapper;
    private byte identifier = 127;


    public ExternalReplicatorBuilder(final Class<V> vClass, boolean wrapTextAndDateFieldsInQuotes) {
        final ExternalReplicator.FieldMapper.ReflectionBasedFieldMapperBuilder<V> fieldMapperBuilder = new ExternalReplicator.FieldMapper.ReflectionBasedFieldMapperBuilder<V>();
        fieldMapperBuilder.wrapTextAndDateFieldsInQuotes(wrapTextAndDateFieldsInQuotes);
        fieldMapper = fieldMapperBuilder.create(vClass, dateTimeFormatter);
    }

    public ExternalReplicator.FieldMapper<V> fieldMapper() {
        return fieldMapper;
    }

    public B fieldMapper(ExternalReplicator.FieldMapper<V> fieldMapper) {
        this.fieldMapper = fieldMapper;
        return (B) this;
    }

    public DateTimeZone dateTimeZone() {
        return dateTimeZone;
    }

    public B dateTimeZone(DateTimeZone dateTimeZone) {
        this.dateTimeZone = dateTimeZone;
        return (B) this;
    }

    public DateTimeFormatter dateTimeFormatter() {
        return dateTimeFormatter;
    }

    public B dateTimeFormatter(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
        return (B) this;
    }

    public DateTimeFormatter shortDateTimeFormatter() {
        return shortDateTimeFormatter;
    }

    public B shortDateTimeFormatter(DateTimeFormatter shortDateTimeFormatter) {
        this.shortDateTimeFormatter = shortDateTimeFormatter;
        return (B) this;
    }

    public String shortDateTimeFormatterStr() {
        return shortDateTimeFormatterStr;
    }

    public B shortDateTimeFormatterStr(String shortDateTimeFormatterStr) {
        this.shortDateTimeFormatterStr = shortDateTimeFormatterStr;
        return (B) this;
    }

    public byte identifer() {
        return this.identifier;
    }

    public B identifier(byte identifier) {
        this.identifier = identifier;
        return (B) this;
    }
}
