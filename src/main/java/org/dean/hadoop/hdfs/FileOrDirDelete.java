package org.dean.hadoop.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * rm [-r]
 */
public class FileOrDirDelete {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(args[0]),configuration);
        fileSystem.delete(new Path(args[0]),false);
    }
}
