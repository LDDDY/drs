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

public class InverseDocumentFrequencyTest {
    @Test
    public void calculateIDF() throws IOException, InterruptedException, ClassNotFoundException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        Configuration conf = cb.getConf();

        Job jobIDF = Job.getInstance(cb.getConf());

        jobIDF.setJarByClass(InverseDocumentFrequency.class);
        jobIDF.setMapperClass(InverseDocumentFrequency.InverseDocumentFrequencyMapper.class);
        jobIDF.setReducerClass(InverseDocumentFrequency.InverseDocumentFrequencyReducer.class);

        jobIDF.setMapOutputKeyClass(Text.class);
        jobIDF.setMapOutputValueClass(IntWritable.class);

        jobIDF.setOutputKeyClass(Text.class);
        jobIDF.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobIDF, new Path(PathsInHDFS.PREPROCESSED_DATA));
        FileOutputFormat.setOutputPath(jobIDF, new Path(PathsInHDFS.IDF_OUTPUT));

        System.exit(jobIDF.waitForCompletion(true)?0:1);
    }
}
