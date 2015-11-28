package de.texttech.csv.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsHelpers {

    public static void copyMerge(String src, String dest) throws IOException {
        Configuration configuration = new Configuration();
        FileSystem hdfs = FileSystem.get(configuration);
        FileUtil.copyMerge(hdfs, new Path(src), hdfs, new Path(dest), false, configuration, null);
    }

}
