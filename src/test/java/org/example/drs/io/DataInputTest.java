package org.example.drs.io;

import org.example.drs.shared.Values;

public class DataInputTest {
    public static void main(String[] args) throws Exception {
        int doc_num = DataInput.preprocess("dataset/bbc", "dataset/preprocessedData");
        System.out.println(DataInput.getCount());
        Values.NUM_OF_DOC = DataInput.getCount();
    }
}
