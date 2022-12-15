package org.example.drs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.Paths;
import org.junit.Test;

import java.io.IOException;

public class WeightingTest {
    @Test
    public void calculateTFIDF() throws IOException, InterruptedException, ClassNotFoundException {
        ConfigurationBuilder cb = new ConfigurationBuilder();

        Configuration conf = cb.getConf();

        Job jobWeighting = Job.getInstance(cb.getConf());

        jobWeighting.setJarByClass(Weighting.class);
        jobWeighting.setMapperClass(Weighting.WeightingMapper.class);
        jobWeighting.setReducerClass(Weighting.WeightingReducer.class);

        jobWeighting.setMapOutputKeyClass(Text.class);
        jobWeighting.setMapOutputValueClass(Text.class);

        jobWeighting.setOutputKeyClass(Text.class);
        jobWeighting.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(jobWeighting, new Path(Paths.TF_OUTCOME));
        FileInputFormat.addInputPath(jobWeighting, new Path(Paths.IDF_OUTCOME));

        FileOutputFormat.setOutputPath(jobWeighting, new Path(Paths.TF_IDF_OUTPUT));

        System.exit(jobWeighting.waitForCompletion(true)?0:1);
    }

}
