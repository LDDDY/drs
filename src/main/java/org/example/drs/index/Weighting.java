package org.example.drs.index;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 计算TF-IDF
 */
public class Weighting {

    public static class WeightingMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * map
         * @param key default
         * @param value ("word", "remainingPart")
         *              remainingPart: TF_OUTCOME "docIdentifier,TF" or IDF_OUTCOME ",IDF"
         * @param context key-out: "word"
         *                value-out: "remainingPart"
         */
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().trim().split("\t");
            if(line.length == 2) {
                String word = line[0];
                String remainingPart = line[1];
                context.write(new Text(word), new Text(remainingPart));
            }
        }
    }

    public static class WeightingReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * reduce
         * @param key "word"
         * @param values ["remainingPart",...]
         *               remainingPart: TF_OUTCOME "docIdentifier,TF" or IDF_OUTCOME ",IDF"
         * @param context key-out: "docIdentifier"
         *                value-out: "word,TF-IDF"
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            double IDF = 0;
            String word = key.toString();
            Map<String, Double> doc_tf = new HashMap<>();

            for(Text val: values) {
                String[] remainingPart = val.toString().trim().split(",");
                if(remainingPart.length == 2) {
                    if(remainingPart[0].equals("")) {
                        IDF = Double.valueOf(remainingPart[1]);
                    } else {
                        doc_tf.put(remainingPart[0], Double.valueOf(remainingPart[1]));
                    }
                }
            }

            for(Map.Entry<String, Double> entry: doc_tf.entrySet()) {
                double TF_IDF = IDF * entry.getValue(); // TF-IDF = IDF * TF
                String docIdentifier = entry.getKey();
                String valueOut = word + "," + TF_IDF;
                context.write(new Text(docIdentifier), new Text(valueOut));
            }
        }
    }
}
