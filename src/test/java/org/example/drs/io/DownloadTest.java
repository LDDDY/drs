package org.example.drs.io;

import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.Paths;

import java.io.IOException;

public class DownloadTest {
    public static void main(String[] args) throws IOException {
        ConfigurationBuilder cb = new ConfigurationBuilder();
        DataDownloader.downloadFromHDFS("outcome", Paths.IDF_OUTCOME, cb.getConf());
    }
}
