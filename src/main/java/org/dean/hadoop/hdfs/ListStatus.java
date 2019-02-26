package org.dean.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ListStatus {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(args[0]),configuration);
        Path[] paths = new Path[args.length];
        for (int i = 0;i < paths.length;i++){
            paths[i] = new Path(args[i]);
        }

        FileStatus[] statuses = fileSystem.listStatus(paths);
        Path[]listedPaths = FileUtil.stat2Paths(statuses);
        for (Path path:listedPaths){
            System.out.println(path);
        }
    }
}
