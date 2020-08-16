package org.apache.flink.training.assignments.domain;

import org.apache.flink.api.java.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class OrderTestResults {

    private static final Logger LOG = LoggerFactory.getLogger(OrderTestResults.class);

    public Map<String, TBillTestRecord> resultMap = new HashMap<>();

    public  static OrderTestResults ofResource(String resource) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(OrderTestResults.class.getResourceAsStream(resource)))) {
            OrderTestResults results = new OrderTestResults();
            String line = null;
            while ((line = reader.readLine()) != null) {
                results.parseString(line);
            }
            return results;
        } catch (Exception e) {
            LOG.error("Exception: {}", e);
        }
        return null;
    }

    private void parseString(String line) {

        String[] tokens = line.split(",");
        if (tokens.length != 4) {
            throw new RuntimeException("Invalid record: " + line);
        }

        String key = "";//OrderTestResults.getKey(LocalDate.parse(tokens[0]).atStartOfDay());

        resultMap.computeIfAbsent(key, k -> new TBillTestRecord());

        resultMap.computeIfPresent(key, (k, r)-> {
            try {
                r.returns.add(Tuple2.of(LocalDate.parse(tokens[0]).atStartOfDay(),tokens[1].length() > 0 ? Double.parseDouble(tokens[1]) : 0.0));
                r.average = tokens[2].length() > 0 ? Double.parseDouble(tokens[2]) : 0.0;
                r.volatility = tokens[3].length() > 0 ? Double.parseDouble(tokens[3]) : 0.0;
                return r;
            } catch (NumberFormatException nfe) {
                throw new RuntimeException("Invalid record: " + line, nfe);
            }
        });
    }
}
