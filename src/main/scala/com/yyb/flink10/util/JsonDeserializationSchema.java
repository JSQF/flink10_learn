package com.yyb.flink10.util;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JsonDeserializationSchema extends AbstractDeserializationSchema<ObjectNode> {


    public static String JSON_PARSE_ERROR = "_JSON_PARSE_ERROR_";

    private ObjectMapper mapper;

    @Override
    public ObjectNode deserialize(byte[] message) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }

        ObjectNode objectNode;
        try {
            objectNode = mapper.readValue(message, ObjectNode.class);
        } catch (Exception e) {
            ObjectMapper errorMapper = new ObjectMapper();
            ObjectNode errorObjectNode = errorMapper.createObjectNode();
            errorObjectNode.put(JSON_PARSE_ERROR, new String(message));
            objectNode = errorObjectNode;
        }
        return objectNode;
    }

    @Override
    public boolean isEndOfStream(ObjectNode nextElement) {
        return false;
    }

}