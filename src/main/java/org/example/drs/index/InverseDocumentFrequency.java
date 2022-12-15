package org.example.drs.index;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.example.drs.shared.Values;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class InverseDocumentFrequency {
    public static class InverseDocumentFrequencyMapper
            extends Mapper<Object, Text, Text, IntWritable> {
        /**
         * map
         * @param key default
         * @param value json object from Path.PREPROCESSED_DATA
         * @param context key-out: "word" value-out: 1
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            JSONObject jsonData = JSON.parseObject(value.toString());
            String text = jsonData.getString("text");
            StringTokenizer words = new StringTokenizer(text, " '\n\t.,!?:()[]{};\\/\"*");

            HashSet<String> uniqueWords = new HashSet<>();
            while (words.hasMoreTokens()) {
                String word = words.nextToken().trim().toLowerCase();
                if (word.equals("") || uniqueWords.contains(word)) {
                    continue;
                }
                uniqueWords.add(word);
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }

    public static class InverseDocumentFrequencyReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        /**
         * reduce
         * @param key "word"
         * @param values [1,...]
         * @param context key-out: "word" value-out: ",IDF"
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {

            int docCount = 0;
            for(IntWritable val: values) {
                docCount += val.get();
            }
            double IDF = Math.log( Values.NUM_OF_DOC / (1 + docCount) );
            String valueOut = "," + IDF;
            context.write(key, new Text(valueOut));
        }
    }
}
