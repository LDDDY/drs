package org.example.drs.io;

import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.PathsInHDFS;

import java.io.IOException;

public class DownloadTest {
    public static void main(String[] args) throws IOException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        DataDownloader.downloadFromHDFS("outcome", PathsInHDFS.SIMILARITY_OUTCOME, cb.getConf());
    }
}
