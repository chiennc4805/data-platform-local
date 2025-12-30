package com.example;

import java.sql.Timestamp;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;

public class DebeziumParser
        implements FlatMapFunction<String, CustomerEvent> {

    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void flatMap(String value,
                        Collector<CustomerEvent> out)
            throws Exception {

        JsonNode root = mapper.readTree(value);

        String op = root.get("op").asText();

        JsonNode dataNode =
                op.equals("d") ? root.get("before") : root.get("after");

        CustomerEvent e = new CustomerEvent();
        e.op = op;

        if (dataNode != null && !dataNode.isNull()) {
            e.id = dataNode.get("id").asInt();
            e.name = dataNode.get("name").asText();
            e.email = dataNode.get("email").asText();
            JsonNode createdNode = dataNode.get("created_at");
            if (createdNode != null && !createdNode.isNull()) {
                long millis = createdNode.asLong();
                e.created_at = new Timestamp(millis);  // e.created_at là java.sql.Timestamp
            } else {
                e.created_at = null;
            }        
        }

        // KHÔNG filter
        // KHÔNG if c/u/d
        out.collect(e);
    }
}

