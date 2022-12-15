package org.example.index;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.StringTokenizer;

public class TermFrequency {

    public static int totalWordsNum; // 一个文档中的总词数，用于词频归一化

    public static class TermFrequencyMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            JSONObject data = JSON.parseObject(value.toString());
            String dirName = data.getString("category");
            String fileName = data.getString("name");
            String docIdentifier = dirName + "/" + fileName;
            String text = data.getString("text");
//            String[] words = text.split(" \'\n.,!?:()[]{};\\/\"*");

            StringTokenizer words = new StringTokenizer(text, " \'\n.,!?:()[]{};\\/\"*");
            int wordsNum = 0;
            while (words.hasMoreTokens()) {
                String word = words.nextToken().toLowerCase();
                if (word.equals("")) {
                    continue;
                }
                wordsNum++;
                context.write(new Text(word + "," + docIdentifier), new DoubleWritable(1));
            }
            totalWordsNum = wordsNum;
        }
    }

    public static class TermFrequencyReducer
            extends Reducer<Text, DoubleWritable, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (DoubleWritable val : values) {
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
