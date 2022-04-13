package com.ruozedata.hadoop.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @author 阿左
 * @theme
 * @create 2021-02-25 3:29
 * @remark
 **/
public class FileUtils {

    public static void deleteTarget(Configuration configuration, String outputFile) throws IOException {

        FileSystem fileSystem = FileSystem.get(configuration);

        //输出文件目录存在
        Path path = new Path(outputFile);
        if(fileSystem.exists(new Path(outputFile))){
            fileSystem.delete(path, true);
        }
    }
}
