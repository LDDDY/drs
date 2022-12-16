package org.example.drs.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.drs.io.DataUploader;
import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.PathsInHDFS;
import org.example.drs.shared.Values;

import java.io.File;

public class IndexDriver {
    private static boolean jobTFCompleted = false;
    private static boolean jobIDFCompleted = false;
    private static boolean jobTFIDFCompleted = false;
    private static boolean jobVectorCompleted = false;

    public static boolean run(String localInputPath) throws Exception {

        ConfigurationBuilder confBuilder = new ConfigurationBuilder();
        Configuration conf = confBuilder.getConf();

        // 删除HDFS中的drs目录或文件
        Path drs = new Path("drs");
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(drs)) {
            fs.delete(drs, true);
        }

        // 检测本地数据集是否存在
        File dataset = new File(localInputPath);
        if(!dataset.exists()) {
            return false;
        }
        // 预处理后的本地输出位置
        String preprocessedDataPath = dataset.getPath() + "/preprocessedData";

        // 数据预处理
        Values.num_of_docs = PerProcessor.preprocess(localInputPath, preprocessedDataPath);

        // 上传到HDFS
        boolean uploaded = DataUploader.uploadToHDFS(preprocessedDataPath, PathsInHDFS.PREPROCESSED_DATA, conf);
        if (!uploaded) {
            return false;
        }

        jobTFCompleted = TermFrequency.runTFCounter(conf);

        if (jobTFCompleted) {
            jobIDFCompleted = InverseDocumentFrequency.runIDFCounter(conf);
        }

        if (jobIDFCompleted) {
            jobTFIDFCompleted = Weighting.runTFIDFCalculator(conf);
        }

        if(jobTFIDFCompleted) {
            jobVectorCompleted = DocumentVectorizer.runDocVectorize(conf);
        }

        return jobVectorCompleted;
    }

}
