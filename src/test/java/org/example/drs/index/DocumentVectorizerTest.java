package org.example.drs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.PathsInHDFS;
import org.junit.Test;

import java.io.IOException;

public class DocumentVectorizerTest {
    @Test public void docVec() throws IOException, InterruptedException, ClassNotFoundException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        Configuration conf = cb.getConf();

        Job jobDocVec = Job.getInstance(cb.getConf());

        jobDocVec.setJarByClass(DocumentVectorizer.class);
        jobDocVec.setMapperClass(DocumentVectorizer.DocumentVectorMapper.class);
        jobDocVec.setReducerClass(DocumentVectorizer.DocumentVectorReducer.class);

        jobDocVec.setMapOutputKeyClass(Text.class);
        jobDocVec.setMapOutputValueClass(Text.class);

        jobDocVec.setOutputKeyClass(Text.class);
        jobDocVec.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobDocVec, new Path(PathsInHDFS.TF_IDF_OUTCOME));
        FileOutputFormat.setOutputPath(jobDocVec, new Path(PathsInHDFS.DOC_VECTOR_OUTPUT));

        System.exit(jobDocVec.waitForCompletion(true)?0:1);
    }
}
