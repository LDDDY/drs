package org.example.drs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.Paths;
import org.junit.Test;

import java.io.IOException;

public class InverseDocumentFrequencyTest {
    @Test
    public void calculateIDF() throws IOException, InterruptedException, ClassNotFoundException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        Configuration conf = cb.getConf();

        Job jobTermFrequency = Job.getInstance(cb.getConf());

        jobTermFrequency.setJarByClass(InverseDocumentFrequency.class);
        jobTermFrequency.setMapperClass(InverseDocumentFrequency.InverseDocumentFrequencyMapper.class);
        jobTermFrequency.setReducerClass(InverseDocumentFrequency.InverseDocumentFrequencyReducer.class);

        jobTermFrequency.setMapOutputKeyClass(Text.class);
        jobTermFrequency.setMapOutputValueClass(IntWritable.class);

        jobTermFrequency.setOutputKeyClass(Text.class);
        jobTermFrequency.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobTermFrequency, new Path(Paths.PREPROCESSED_DATA));
        FileOutputFormat.setOutputPath(jobTermFrequency, new Path(Paths.IDF_OUTPUT));

        System.exit(jobTermFrequency.waitForCompletion(true)?0:1);
    }
}
