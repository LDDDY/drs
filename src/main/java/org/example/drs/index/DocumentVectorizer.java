package org.example.drs.index;

import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.PathsInHDFS;

import java.io.IOException;

/**
 * 文档向量化
 * input: TF_IDF_OUTCOME ("docIdentifier", "word,TF-IDF")
 * output: DOC_VECTOR_OUTPUT
 * key-out: docIdentifier
 * value-out: {"word1": TF-IDF1, "word2": TF-IDF2, ...}
 */
public class DocumentVectorizer {
    public static class DocumentVectorMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * map
         * @param value ("docIdentifier", "word,TF-IDF")
         * @param context key-out: "docIdentifier"
         *                value-out: "word,TF-IDF"
         */
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            String[] line = value.toString().trim().split("\t");
            if(line.length == 2) {
                Text docIdentifier = new Text(line[0]);
                Text remainingPart = new Text(line[1]);
                context.write(docIdentifier, remainingPart);
            }
        }
    }

    public static class DocumentVectorReducer extends Reducer<Text, Text, Text, Text> {

        /**
         *
         * @param key "docIdentifier"
         * @param values ["word1,TF-IDF1", "word2,TF-IDF2", ...]
         * @param context key-out: docIdentifier
         *                value-out: {"word1": TF-IDF1, "word2": TF-IDF2, ...}
         */
        @Override
        public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {

            JSONObject jsonData = new JSONObject();
            for(Text val: values) {
                String[] word_tfidf = val.toString().trim().split(",");
                if(word_tfidf.length == 2) {
                    jsonData.put(word_tfidf[0], word_tfidf[1]);
                }
            }
            context.write(key, new Text(jsonData.toString()));
        }
    }

    public static boolean runDocVectorize(Configuration conf)
            throws IOException, InterruptedException, ClassNotFoundException {

        Job jobDocVec = Job.getInstance(conf);

        jobDocVec.setJarByClass(DocumentVectorizer.class);
        jobDocVec.setMapperClass(DocumentVectorizer.DocumentVectorMapper.class);
        jobDocVec.setReducerClass(DocumentVectorizer.DocumentVectorReducer.class);

        jobDocVec.setMapOutputKeyClass(Text.class);
        jobDocVec.setMapOutputValueClass(Text.class);

        jobDocVec.setOutputKeyClass(Text.class);
        jobDocVec.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobDocVec, new Path(PathsInHDFS.TF_IDF_OUTCOME));
        FileOutputFormat.setOutputPath(jobDocVec, new Path(PathsInHDFS.DOC_VECTOR_OUTPUT));

        return jobDocVec.waitForCompletion(true);
    }
}
