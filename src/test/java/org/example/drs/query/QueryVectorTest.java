package org.example.drs.query;

import org.apache.hadoop.conf.Configuration;
import org.example.drs.shared.ConfigurationBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryVectorTest {
    @Test public void testVec() throws IOException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        Configuration conf = cb.getConf();
        String query = "The dollar has hit its highest level";
        Map<String, Double> map = QueryVectorizer.vectorize(query, conf);
        System.out.println(map.toString());
    }
}
