/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.source.ram.expandjsondbz;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main project class implementing JSON string transformation.
 */
abstract class ExpandJSON<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExpandJSON.class);

    interface ConfigName {
        String SOURCE_FIELDS = "sourceFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.SOURCE_FIELDS, ConfigDef.Type.LIST, "", ConfigDef.Importance.MEDIUM,
                    "Source field name. This field will be expanded to json object.");

    private static final String PURPOSE = "json field expansion";

    private List<String> sourceFields;
    Map<String, HashSet<String>> convertlist = new HashMap<>();


    private final String delimiterSplit = "\\.";
    private final String delimiterJoin = ".";

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        sourceFields = config.getList(ConfigName.SOURCE_FIELDS);
//        LOGGER.info("[ramkrish] convertlist:" + sourceFields);
        for (String field : sourceFields) {
//            LOGGER.info("[ramkrish] field:" + field);
            String[] parts = field.split("@");
//            LOGGER.info("[ramkrish] convertlist:" + parts[0]);
//            LOGGER.info("[ramkrish] convertlist:" + parts[1]);
            if (convertlist.containsKey(parts[0])) {
//                LOGGER.info("[ramkrish] convertlist old key");
                HashSet<String> tmp = convertlist.get(parts[0]);
                tmp.add(parts[1]);
                convertlist.put(parts[0], tmp);
                // Okay, there's a key but the value is null
            } else {
//                LOGGER.info("[ramkrish] convertlist new key");
                HashSet<String> tmp = new HashSet<String>() ;
                tmp.add(parts[1]);
                convertlist.put(parts[0], tmp);
                // Definitely no such key
            }

        }
//        LOGGER.info("[ramkrish] convertlist: after" + convertlist);
//        LOGGER.info("[ramkrish] sourceFields:");
//        LOGGER.info(sourceFields.toString());
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
//            LOGGER.info("[ramkrish]Schemaless records not supported");
            return null;
        } else {
//            LOGGER.info("[ramkrish] record:");
//            LOGGER.info(record.toString());
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        try {
            Object recordValue = operatingValue(record);
            if (recordValue == null) {
//                LOGGER.info("[ramkrish] Record");
//                LOGGER.info(record.toString());
                return record;
            }

//            LOGGER.info("[ramkrish] Record");
//            LOGGER.info(record.toString());
//
//            LOGGER.info("[ramkrish] sourceFields : " + sourceFields);

            final Struct value = requireStruct(recordValue, PURPOSE);

//            LOGGER.info("[ramkrish] Source value " + value.get(sourceFields.toString()));

//            LOGGER.info("[ramkrish]  GOT A BREAK TOPICS " + convertlist);
//
//            LOGGER.info("[ramkrish]  GOT A BREAK TOPICS " + record.topic());
//
//            LOGGER.info("[ramkrish]  GOT A BREAK TOPICS " + convertlist.get(record.topic()));



//            for (Field field : value.schema().fields()) {
//                LOGGER.info("[ramkrish]  field :" + field.name());
//                LOGGER.info("[ramkrish]  value :" + value.get(field));
//            }

            Schema schema = value.schema();
//            List<String> convert = new ArrayList<>();
            HashSet<String> convert = new HashSet<String>() ;
            for (Field field: schema.fields()) {
//                LOGGER.info("[ramkrish]  name :" + field.name() + " schema : " + field.schema().toString() + " value :" + value.get(field));
                if (Objects.equals(String.valueOf(field.schema()), "Schema{io.debezium.data.Json:STRING}")){
                    convert.add(field.name());
//                    LOGGER.info("[ramkrish]  convert is :" + field.name());
                }
            }
//            LOGGER.info("[ramkrish]  convert :" + convert);
            HashSet<String> overall_field = new HashSet<String>() ;
            overall_field.addAll(convert);
            if (convertlist.containsKey(record.topic())) {
                overall_field.addAll(convertlist.get(record.topic()));
            }

            List<String> field_list = new ArrayList<String>(overall_field);

//            LOGGER.info("[ramkrish]  GOT A BREAK TOPICS " + field_list);
            final HashMap<String, BsonDocument> jsonParsedFields = parseJsonFields(value, field_list, delimiterSplit);
            final Schema updatedSchema = makeUpdatedSchema(null, value, jsonParsedFields);
            final Struct updatedValue = makeUpdatedValue(null, value, updatedSchema, jsonParsedFields);
            return newRecord(record, updatedSchema, updatedValue);

        } catch (DataException e) {
//            LOGGER.warn("ExpandJSON fields missing from record: " + record.toString(), e);
            return record;
        }
    }

    private static String getStringValue(List<String> path, Struct value) {
        if (path.isEmpty()) {
            return null;
        } else if (path.size() == 1) {
            return value.getString(path.get(0));
        } else {
            return getStringValue(path.subList(1, path.size()), value.getStruct(path.get(0)));
        }
    }

    /**
     * Parse JSON objects from given string fields.
     * @param value Input record to read original string fields.
     * @param sourceFields List of fields to parse JSON objects from.
     * @return Collection of parsed JSON objects with field names.
     */
    private static HashMap<String, BsonDocument> parseJsonFields(Struct value, List<String> sourceFields,
                                                                 String levelDelimiter) {
        final HashMap<String, BsonDocument> bsons = new HashMap<>(sourceFields.size());
        for(String field : sourceFields){
            BsonDocument val;
            String[] pathArr = field.split(levelDelimiter);
            List<String> path = Arrays.asList(pathArr);
            final String jsonString = getStringValue(path, value);
            if (jsonString == null) {
                val = null;
            } else {
                try {
                    if (jsonString.startsWith("{")) {
                        val = BsonDocument.parse(jsonString);
                    } else if (jsonString.startsWith("[")) {
                        final BsonArray bsonArray = BsonArray.parse(jsonString);
                        val = new BsonDocument();
                        val.put("array", bsonArray);
                    } else {
                        String msg = String.format("Unable to parse filed '%s' starting with '%s'", field, jsonString.charAt(0));
                        throw new Exception(msg);
                    }
                } catch (Exception ex) {
                    LOGGER.warn(ex.getMessage(), ex);
                    val = new BsonDocument();
                    val.put("value", new BsonString(jsonString));
                    val.put("error", new BsonString(ex.getMessage()));
                }
            }
            bsons.put(field, val);
        }
        return bsons;
    }

    /**
     * Copy original fields value or take parsed JSONS from collection.
     * @param value Input value to copy fields from.
     * @param updatedSchema Schema for new output record.
     * @param jsonParsedFields Parsed JSON objects.
     * @return Output record with parsed JSON values.
     */
    private Struct makeUpdatedValue(String parentKey, Struct value, Schema updatedSchema, HashMap<String, BsonDocument> jsonParsedFields) {
        final Struct updatedValue = new Struct(updatedSchema);
        for (Field field : value.schema().fields()) {
            final Object fieldValue;
            final String absoluteKey = joinKeys(parentKey, field.name());
            if (jsonParsedFields.containsKey(absoluteKey)) {
                final BsonDocument parsedValue = jsonParsedFields.get(absoluteKey);
                fieldValue = DataConverter.jsonStr2Struct(parsedValue,
                        updatedSchema.field(field.name()).schema());
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldValue = makeUpdatedValue(absoluteKey, value.getStruct(field.name()),
                        updatedSchema.field(field.name()).schema(), jsonParsedFields);
            } else {
                fieldValue = value.get(field.name());
            }
            updatedValue.put(field.name(), fieldValue);
        }
        return updatedValue;
    }

    private String joinKeys(String parent, String child) {
        if (parent == null) {
            return child;
        }
        return parent + delimiterJoin + child;
    }

    /**
     * Update schema using JSON template from config.
     * @param value Input value to take basic schema from.
     * @param jsonParsedFields Values of parsed json string fields.
     * @return New schema for output record.
     */
    private Schema makeUpdatedSchema(String parentKey, Struct value, HashMap<String, BsonDocument> jsonParsedFields) {
        final SchemaBuilder builder = SchemaBuilder.struct();
        for (Field field : value.schema().fields()) {
            final Schema fieldSchema;
            final String absoluteKey = joinKeys(parentKey, field.name());
            if (jsonParsedFields.containsKey(absoluteKey)) {
                fieldSchema = SchemaParser.bsonDocument2Schema(jsonParsedFields.get(absoluteKey));
            } else if (field.schema().type().equals(Schema.Type.STRUCT)) {
                fieldSchema = makeUpdatedSchema(absoluteKey, value.getStruct(field.name()), jsonParsedFields);
            } else {
                fieldSchema = field.schema();
            }
            builder.field(field.name(), fieldSchema);
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() { }

    protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Value<R extends ConnectRecord<R>> extends ExpandJSON<R> {

        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
