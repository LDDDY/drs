package org.example.drs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.PathsInHDFS;
import org.junit.Test;

import java.io.IOException;


public class TermFrequencyTest {
    @Test
    public void calculateTF() throws IOException, InterruptedException, ClassNotFoundException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        Configuration conf = cb.getConf();

        Job jobTF = Job.getInstance(cb.getConf());

        jobTF.setJarByClass(TermFrequency.class);
        jobTF.setMapperClass(TermFrequency.TermFrequencyMapper.class);
        jobTF.setReducerClass(TermFrequency.TermFrequencyReducer.class);

        jobTF.setMapOutputKeyClass(Text.class);
        jobTF.setMapOutputValueClass(IntWritable.class);

        jobTF.setOutputKeyClass(Text.class);
        jobTF.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobTF, new Path(PathsInHDFS.PREPROCESSED_DATA));
        FileOutputFormat.setOutputPath(jobTF, new Path(PathsInHDFS.TF_OUTPUT));

        System.exit(jobTF.waitForCompletion(true)?0:1);
    }
}
