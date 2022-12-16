package org.example.drs.query;

import org.example.drs.shared.ConfigurationBuilder;
import org.junit.Test;

import java.io.IOException;

public class SimilarTest {

    @Test public void testSimilar() throws IOException, InterruptedException, ClassNotFoundException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        String qs = "Revenues at media group Reuters slipped";
        SimilarityCalculator.runCalculator(qs, cb.getConf());
    }
}
