package org.example;

import org.apache.hadoop.conf.Configuration;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) {
        Configuration conf = new Configuration();

        // HDFS相关配置
        conf.set("fs.defaultFS","hdfs://localhost:9000");
        conf.setBoolean("dfs.support.append", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
    }
}
