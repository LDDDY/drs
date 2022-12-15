package org.example.drs.io;

import org.apache.hadoop.conf.Configuration;
import org.example.drs.shared.FileManager;

import java.io.IOException;

public class DataUploader {

    /**
     * 将文件上传至HDFS指定目录
     * 出现重名文件，则新文件覆盖旧文件
     * 若无重名文件，则正常上传
     * @param localPath 本地文件的位置
     * @param targetPath 上传至HDFS的目标位置
     * @param conf 配置
     * @return 上传成功返回true，失败返回false
     */
    public static boolean uploadToHDFS(String localPath, String targetPath, Configuration conf)
            throws IOException {
        FileManager fm = new FileManager(localPath, targetPath, conf);
        return fm.upload(true);
    }

}
