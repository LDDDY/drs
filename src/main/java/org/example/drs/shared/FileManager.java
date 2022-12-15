package org.example.drs.shared;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileManager {
    private final String localPathStr; // 绝对路径
    private final String hdfsPathStr;
    FileSystem fs;

    public FileManager(String localPath, String hdfsPath, Configuration conf) throws IOException {
        hdfsPathStr = hdfsPath;
        localPathStr = localPath;
        fs = FileSystem.get(conf);
    }

    /**
     * 检查要上传的文件是否与HDFS目标路径中的文件重名
     * @return 文件存在于HDFS则返回true, 否则返回false
     */
    public boolean checkHDFSExistenceForUpload() throws IOException {
        File localFile = new File(localPathStr);
        String hdfsFileName = localFile.getName();
        String checkedPath = hdfsPathStr + "/" + hdfsFileName;
        return fs.exists(new Path(checkedPath));
    }

    /**
     * 从本地向HDFS上传文件
     * 若文件已存在于HDFS, 则按照预设参数进行追加或覆盖操作
     * @param copyMode 为true则直接覆盖原文件; 为false则追加到原文件后
     */
    public boolean upload(boolean copyMode) throws IOException {
        Path hdfsPath = new Path(hdfsPathStr);
        Path localPath = new Path(localPathStr);
        File localFile = new File(localPathStr);
        // 检查本地文件是否存在
        if(!localFile.exists()){
            return false;
        }

        if(checkHDFSExistenceForUpload() && !copyMode){
            java.nio.file.Path path = Paths.get(localPathStr);
            byte[] bytes = Files.readAllBytes(path);
            String hdfsFileName = localPathStr
                    .substring(localPathStr.lastIndexOf("/") + 1);
            String hdfsFilePathStr = hdfsPathStr + "/" + hdfsFileName;
            Path hdfsFilePath = new Path(hdfsFilePathStr);
            FSDataOutputStream os = fs.append(hdfsFilePath);
            os.write(bytes);
            os.close();
        }
        else {
            fs.copyFromLocalFile(false, copyMode, localPath, hdfsPath);
        }
        return true;
    }

    /**
     * 检查是否与本地文件重名
     * @return 重名返回true, 否则返回false
     */
    public boolean checkLocalExistenceForDownload(){
        Path hdfsFilePath = new Path(hdfsPathStr);
        String fileName = hdfsFilePath.getName();
        Set<String> localFilenames = getLocalFileNames(localPathStr);
        return localFilenames.contains(fileName);
    }

    private Set<String> getLocalFileNames(String dirPathStr) {
        File dir = new File(dirPathStr);
        Set<String> fileNameSet = new HashSet<>();
        if(!dir.isDirectory()){
            return fileNameSet;
        }
        File[] files = dir.listFiles();
        if(files == null || files.length == 0) {
            return fileNameSet;
        }
        for(File f: files) {
            fileNameSet.add(f.getName());
        }
        return fileNameSet;
    }

    public boolean download() throws IOException {
        // 检查文件是否存在于HDFS
        Path hdfsFilePath = new Path(hdfsPathStr);
        if(!fs.exists(hdfsFilePath)) {
            return false;
        }
        // 检查是否与本地文件重名
        if(!checkLocalExistenceForDownload()) {
            fs.copyToLocalFile(false,
                    hdfsFilePath,
                    new Path(localPathStr));
        }
        else {
            String fileNameCopy =
                    generateNameForFileCopy(hdfsFilePath.getName());

            String fmtLocalPathStr = this.localPathStr;
            if(!fmtLocalPathStr.endsWith("/")) {
                fmtLocalPathStr = fmtLocalPathStr + "/";
            }
            fmtLocalPathStr = fmtLocalPathStr + fileNameCopy;
            Path localPath = new Path(fmtLocalPathStr);
            fs.copyToLocalFile(false, hdfsFilePath, localPath);
        }
        return true;
    }

    private String generateNameForFileCopy(String name) {
        String fileCopyName;
        // 检查是否存在后缀
        if(name.contains(".")) {
            String pureName = name.substring(0, name.indexOf("."));
            String suffix = name.substring(name.indexOf("."));
            pureName = pureName + "-copy";
            fileCopyName = pureName + suffix;
        }
        else {
            fileCopyName = name + "-copy";
        }
        return fileCopyName;
    }
}
