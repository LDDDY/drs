package org.example.io;

import org.example.shared.Values;

public class DataInputTest {
    public static void main(String[] args) throws Exception {
        DataInput dataIn = DataInput.getInstance("dataset/bbc", "dataset/preprocessedData");
        System.out.println(dataIn.getCount());
        Values.NUM_OF_DOC = dataIn.getCount();
    }
}
