package org.example.drs.io;

import org.apache.hadoop.conf.Configuration;
import org.example.drs.shared.FileManager;

import java.io.IOException;

public class DataDownloader {
    /**
     * 从HDFS中下载文件到本地
     * @param localPath 本地下载目录
     * @param hdfsPath HDFS目录
     * @param conf 配置
     * @return 下载成功返回true，失败返回false
     * @throws IOException
     */
    public static boolean downloadFromHDFS(String localPath, String hdfsPath, Configuration conf)
            throws IOException {

        FileManager fm = new FileManager(localPath, hdfsPath, conf);
        return fm.download();
    }
}
