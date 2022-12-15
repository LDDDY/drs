package org.example.io;

import org.apache.hadoop.conf.Configuration;
import org.example.shared.FileManager;
import org.example.shared.Paths;

import java.io.IOException;

public class DataUploader {

    /**
     * 将文件上传至HDFS指定目录
     * 出现重名文件，则新文件覆盖旧文件
     * 若无重名文件，则正常上传
     * @param localPath 本地文件的位置
     * @param targetPath 上传至HDFS的位置
     */
    public static boolean uploadToHDFS(String localPath, String targetPath, Configuration conf)
            throws IOException {
        FileManager fm = new FileManager(localPath, targetPath, conf);
        return fm.upload(true);
    }

//    public static void main(String[] args) throws IOException {
//        Configuration conf = new Configuration();
//
//        // HDFS相关配置
//        conf.set("fs.defaultFS","hdfs://localhost:9000");
//        conf.setBoolean("dfs.support.append", true);
//        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
//        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
//
//        DataUploader.uploadToHDFS("~/preprocessedData", Paths.PREPROCESSED_DATA, conf);
//    }
}
