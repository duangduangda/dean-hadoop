package org.dean.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class FilePatternFilter {
    public static void main(String[] args) throws Exception {
        Path pathPattern = new Path(args[0]);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(args[0]),configuration);
        FileStatus[] fileStatuses = fileSystem.globStatus(pathPattern);
        Path[]listedPaths = FileUtil.stat2Paths(fileStatuses);
        for (Path path:listedPaths){
            System.out.println(path);
        }
    }
}
