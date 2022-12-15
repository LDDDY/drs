package org.example.io;

import com.alibaba.fastjson2.JSONObject;
import java.io.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class DataInput {

    private static DataInput singletonDataInput = null;
    private String datasetPath;

    private DataInput(String dataPath) {
        this.datasetPath = dataPath;
    }

    private int count = 0;

    public void writeJson(String jsonPath, Map<String, Object> fileMap, boolean flag) throws Exception{
        String data = new JSONObject(fileMap).toString();
        File jsonFile = new File(jsonPath);
        if(!jsonFile.exists()){
            jsonFile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(jsonFile.getAbsoluteFile(),flag);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(data);
        bufferedWriter.write("\n");//使json文件有换行
        bufferedWriter.close();
    }

    public List<Map<String,Object>> getFileMap() throws Exception{
        File file = new File(datasetPath);
        File[] fileList = file.listFiles();
        List<Map<String,Object>> fileMaps = new ArrayList<>();
        //System.out.println(fileList.length);
        for (int i = 0; i < fileList.length; i++) {
            //System.out.println(fileList[i]);
            if(fileList[i].isDirectory()){
                File file2 = new File(fileList[i].toString());
                File[] fileList2 = file2.listFiles();
                String category = fileList[i].toString();
                int categoryStart = category.lastIndexOf("/");
                category = category.substring(categoryStart+1);//取相对路径作为category
                System.out.print(category+":");
                System.out.println(fileList2.length);
                for (int j = 0; j < fileList2.length; j++) {
                    //System.out.println(fileList2[j]);
                    String name = fileList2[j].toString();
                    int nameStart = name.lastIndexOf("/");
//                    int nameEnd = name.lastIndexOf(".");
//                    name = name.substring(nameStart+1, nameEnd);
                    name = name.substring(nameStart+1); //取文件名作为name
                    Map<String,Object> fileMap = new LinkedHashMap<>();
                    BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(fileList2[j]),"utf-8"));

                    fileMap.put("category",category);
                    fileMap.put("name",name);
                    String txt = null;
                    StringBuilder sb = new StringBuilder();
                    while((txt = br.readLine())!=null){
                        sb.append(txt).append("\r\n");
                    }
                    br.close();
                    fileMap.put("text",sb);
                    fileMaps.add(fileMap);
                    //System.out.println(fileMap);
                }
            }
        }
        return fileMaps;
    }

    public int getCount() {
        return count;
    }

    public static DataInput getInstance(String dataPath, String outPath) throws Exception {

        if(singletonDataInput == null) {
            singletonDataInput = new DataInput(dataPath);
        }

        List<Map<String,Object>> fileMaps=singletonDataInput.getFileMap();
        int counter = 0;
        File jsonFile = new File(outPath);
        for (Map<String,Object> fileMap:fileMaps){
            singletonDataInput.writeJson(jsonFile.getAbsolutePath(),fileMap,true);
            counter++;
        }
        singletonDataInput.count = counter;
        return singletonDataInput;
    }
}
