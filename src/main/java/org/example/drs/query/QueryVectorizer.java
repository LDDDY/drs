package org.example.drs.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.drs.shared.PathsInHDFS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class QueryVectorizer {

    /**
     * 查询向量化
     * 查询向量中的单词满足同时存在于查询字符串与数据集
     * 查询单词的TF为查询字符串中的归一化的词频
     * IDF为数据集单词的逆文本频率
     * @param query 查询字符串
     * @param conf 配置
     * @return 查询单词与其TF-IDF的映射
     */
    public static Map<String, Double> vectorize(String query, Configuration conf) throws IOException {

        StringTokenizer queryWords = new StringTokenizer(query.toLowerCase(), " '\n\t.,!?:()[]{};\\/\"*");
        HashMap<String, Integer> queryWord_TF = new HashMap<>();
        HashMap<String, Double> queryVec = new HashMap<>();
        int queryWordsSum = 0; // 查询字符串的总词数
        // 求查询字符串的词频
        while (queryWords.hasMoreTokens()) {
            String qword = queryWords.nextToken();
            queryWordsSum++;
            if(queryWord_TF.containsKey(qword)) {
                queryWord_TF.put(qword, queryWord_TF.get(qword) + 1);
            } else {
                queryWord_TF.put(qword, 1);
            }
        }

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream idf = fs.open(new Path(PathsInHDFS.IDF_OUTCOME));
        BufferedReader br = new BufferedReader(new InputStreamReader(idf));
        String line = br.readLine();
        while (line != null) {
            String[] word_IDF = line.trim().split("\t");
            if(word_IDF.length == 2) {
                String word = word_IDF[0];
                Double IDF = Double.valueOf(word_IDF[1].substring(1));
                if(queryWord_TF.containsKey(word)) {
                    // 将查询单词在查询字符串中的词频归一化，并将其与IDF相乘得到查询单词的TF-IDF
                    queryVec.put(word, queryWord_TF.get(word) * IDF / queryWordsSum);
                }
            }
            line = br.readLine();
        }

        return queryVec;
    }
}
