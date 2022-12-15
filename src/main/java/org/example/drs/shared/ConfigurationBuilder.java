package org.example.drs.shared;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationBuilder {
    private static Configuration conf;

    public ConfigurationBuilder() {
        conf = new Configuration();

        // HDFS相关配置
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
    }

    public Configuration getConf() {
        return conf;
    }
}
