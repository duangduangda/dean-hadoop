package org.dean.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * copyFromLocal
 */
public class FileCopyWithProgress {
    public static void main(String[] args) throws Exception{
        String src = args[0];
        String dest = args[1];
        InputStream inputStream = new BufferedInputStream(new FileInputStream(src));
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create(dest),configuration);
        OutputStream outputStream = fileSystem.create(new Path(dest), new Progressable() {
            public void progress() {
                System.out.printf("*");
            }
        });
        IOUtils.copyBytes(inputStream,outputStream,4096,true);
    }
}
