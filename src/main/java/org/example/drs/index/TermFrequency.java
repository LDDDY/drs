package org.example.drs.index;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.PathsInHDFS;

import java.io.IOException;
import java.util.StringTokenizer;

public class TermFrequency {

    public static int totalWordsNum; // 一个文档中的总词数，用于词频归一化

    public static class TermFrequencyMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        /**
         * map
         * @param value value-in: json object from Path.PREPROCESSED_DATA
         * @param context key-out: "word,docIdentifier"
         *                value-out: 1
         */
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
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
         * @param context key-out: "word"
         *                value-out: "docIdentifier,TF"
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            double sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            String[] keySplits = key.toString().split(",");
            if(keySplits.length == 2) {
                Text outKey = new Text(keySplits[0]); // 单词
                double TF = sum / totalWordsNum;
                Text outVal = new Text(keySplits[1] + "," + TF); // docIdentifier,TF
                context.write(outKey, outVal);
            }
        }
    }

    public static boolean runTFCounter(Configuration conf)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job jobTF = Job.getInstance(conf);

        jobTF.setJarByClass(TermFrequency.class);
        jobTF.setMapperClass(TermFrequency.TermFrequencyMapper.class);
        jobTF.setReducerClass(TermFrequency.TermFrequencyReducer.class);

        jobTF.setMapOutputKeyClass(Text.class);
        jobTF.setMapOutputValueClass(IntWritable.class);

        jobTF.setOutputKeyClass(Text.class);
        jobTF.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobTF, new Path(PathsInHDFS.PREPROCESSED_DATA));
        FileOutputFormat.setOutputPath(jobTF, new Path(PathsInHDFS.TF_OUTPUT));

        return jobTF.waitForCompletion(true);
    }
}
