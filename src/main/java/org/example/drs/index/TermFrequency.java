package org.example.drs.index;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class TermFrequency {

    public static int totalWordsNum; // 一个文档中的总词数，用于词频归一化

    public static class TermFrequencyMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * map
         * @param key key-in: default
         * @param value value-in: json object from Path.PREPROCESSED_DATA
         * @param context key-out: "word,docIdentifier" value-out: 1
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {

            JSONObject jsonData = JSON.parseObject(value.toString());
            String dirName = jsonData.getString("category");
            String fileName = jsonData.getString("name");
            String docIdentifier = dirName + "/" + fileName;
            String text = jsonData.getString("text");

            StringTokenizer words = new StringTokenizer(text, " '\n\t.,!?:()[]{};\\/\"*");
            int wordsNum = 0;
            while (words.hasMoreTokens()) {
                String word = words.nextToken().trim().toLowerCase();
                if (word.equals("")) {
                    continue;
                }
                wordsNum++;
                context.write(new Text(word + "," + docIdentifier), new IntWritable(1));
            }
            totalWordsNum = wordsNum;
        }
    }

    public static class TermFrequencyReducer
            extends Reducer<Text, IntWritable, Text, Text> {
        /**
         * reduce
         * @param key word,docIdentifier
         * @param values [1,...]
         * @param context key-out: "word" value-out: "docIdentifier,TF"
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String[] keySplits = key.toString().split(",");
            Text outKey = new Text(keySplits[0]); // 单词
            double TF = sum / totalWordsNum;
            Text outVal = new Text(keySplits[1] + "," + TF); // docIdentifier,TF
            context.write(outKey, outVal);
        }
    }
}
