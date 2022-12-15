package org.example.drs.io;


import org.apache.hadoop.conf.Configuration;
import org.example.drs.shared.Paths;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import java.io.File;
import java.io.IOException;

public class DataUploaderTest {

    @Test
    public void uploadTest() throws IOException {
        Configuration conf = new Configuration();

        // HDFS相关配置
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);

        String path = "dataset/preprocessedData";

        boolean b = DataUploader.uploadToHDFS(path, Paths.PREPROCESSED_DATA, conf);
        File f = new File(path);
        if(f.exists()) {
            System.out.println("localFile exist");
        } else  {
            System.out.println("No local");
        }

        if(b) {
            System.out.println("uploaded");
        }
        else {
            System.out.println("Failed");
        }
    }

    public static void main(String args[]) throws IOException {
        Result result = JUnitCore.runClasses(DataUploaderTest.class);
        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
        System.out.println(result.wasSuccessful());
//        DataUploaderTest uploader = new DataUploaderTest();
//        uploader.uploadTest();

    }
}
