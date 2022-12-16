package org.example.drs.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.drs.shared.ConfigurationBuilder;
import org.example.drs.shared.PathsInHDFS;

import java.io.IOException;

public class QueryDriver {
    public static boolean query(String queryString, int N)
            throws IOException, InterruptedException, ClassNotFoundException {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        Configuration conf = cb.getConf();

        // 检查是否已经进行索引
        FileSystem fs = FileSystem.get(conf);
        boolean indexed =
                fs.exists(new Path(PathsInHDFS.IDF_OUTCOME)) &&
                fs.exists(new Path(PathsInHDFS.DOC_VECTOR_OUTCOME));

        if(!indexed) {
            return false;
        }

        // 删除上次查询的输出
        Path simPath = new Path(PathsInHDFS.SIMILARITY_OUTPUT);
        if(fs.exists(simPath)) {
            fs.delete(simPath, true);
        }

        boolean queryStatus = SimilarityCalculator.runCalculator(queryString, conf);
        if (!queryStatus) {
            return false;
        }

        Ranker.rank(N, conf);
        return true;
    }
}
