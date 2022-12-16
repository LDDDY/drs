package org.example.drs.io;

import org.example.drs.index.PerProcessor;
import org.example.drs.shared.Values;

public class DataInputTest {
    public static void main(String[] args) throws Exception {
        int doc_num = PerProcessor.preprocess("dataset/bbc", "dataset/preprocessedData");
        System.out.println(PerProcessor.getCount());
        Values.num_of_docs = PerProcessor.getCount();
    }
}
