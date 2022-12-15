package org.example.drs.io;

import org.junit.Test;

import java.io.File;
import java.nio.file.Path;

public class PathTest {
    @Test public void testPath() {
        String localInputPath = "/home/ldy/";

        // 检测本地数据集是否存在
        File dataset = new File(localInputPath);

        //预处理后的本地输出位置
        String preprocessedDataPath = dataset.getPath() + "/preprocessedData";
        System.out.println(preprocessedDataPath);
    }
}
