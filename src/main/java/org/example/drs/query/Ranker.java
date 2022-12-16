package org.example.drs.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.drs.shared.PathsInHDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Ranker {
    public static void rank(int resNum, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream similarityScore = fs.open(new Path(PathsInHDFS.SIMILARITY_OUTCOME));
        BufferedReader br = new BufferedReader(new InputStreamReader(similarityScore));

        String line = br.readLine();
        int count = 1;

        System.out.println("Document location in dataset\tRank");
        while (line != null && count <= resNum) {
            String docIdentifier = line.trim().split("\t")[0];
            System.out.println(docIdentifier + "\t" + count);
            count++;
            line = br.readLine();
        }
    }
}
