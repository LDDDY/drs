package org.example.shared;

import org.apache.hadoop.conf.Configuration;

public class Conf {
    private static Configuration conf = new Configuration();

    private static void setConf() {
        // HDFS相关配置
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
    }

    public static Configuration getConf() {
        return conf;
    }
}
