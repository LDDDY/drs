package org.example.drs.query;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.PathsInHDFS;
import java.io.IOException;
import java.util.Map;

/**
 * 计算查询字符串与数据集中文档的相似度，并进行排名
 */
public class SimilarityCalculator {

    public static Map<String, Double> queryVector;

    public static class SimilarityMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        /**
         * map
         * 对于一个单词，计算用于排名的score = queryTF-IDF * TF-IDF
         * @param value DOC_VECTOR_OUTCOME
         *              (docIdentifier, {"word1": TF-IDF1, "word2": TF-IDF2, ...})
         * @param context key-out: (-1) * score
         *                value-out: docIdentifier
         */
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, DoubleWritable, Text>.Context context)
                throws IOException, InterruptedException {

            String[] line = value.toString().trim().split("\t");
            if(line.length == 2) {
                Text docIdentifier = new Text(line[0]);
                JSONObject docVector = JSON.parseObject(line[1]);
                for(Map.Entry<String, Double> entry: queryVector.entrySet()) {
                    String word = entry.getKey();
                    if(docVector.containsKey(word)) {
                        double score = entry.getValue() * docVector.getDoubleValue(word);
                        context.write(new DoubleWritable(-1 * score), docIdentifier);
                    }
                }
            }
        }
    }

    public static class SimilarityReducer extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Reducer<DoubleWritable, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException {
            Text docIdentifier = values.iterator().next();
            DoubleWritable score = new DoubleWritable(-1 * key.get());
            context.write(docIdentifier, score);
        }
    }

    public static boolean runCalculator(String query, Configuration conf)
            throws IOException, InterruptedException, ClassNotFoundException {

        queryVector = QueryVectorizer.vectorize(query, conf);

        Job jobSimilar = Job.getInstance(conf);

        jobSimilar.setJarByClass(SimilarityCalculator.class);
        jobSimilar.setMapperClass(SimilarityMapper.class);
        jobSimilar.setReducerClass(SimilarityReducer.class);

        jobSimilar.setMapOutputKeyClass(DoubleWritable.class);
        jobSimilar.setMapOutputValueClass(Text.class);

        jobSimilar.setOutputKeyClass(Text.class);
        jobSimilar.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(jobSimilar, new Path(PathsInHDFS.DOC_VECTOR_OUTCOME));
        FileOutputFormat.setOutputPath(jobSimilar, new Path(PathsInHDFS.SIMILARITY_OUTPUT));

        return jobSimilar.waitForCompletion(true);
    }
}
