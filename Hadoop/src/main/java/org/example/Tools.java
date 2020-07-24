package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class Tools {

    public static FileSystem GetFileSystem() throws IOException {
        String uri = "hdfs://ld1:9000";
        Configuration config = new Configuration();
//        config.addDefaultResource("core-site.xml");
//        config.addDefaultResource("hdfs-site.xml");
//        config.addDefaultResource("yarn-site.xml");
//        config.addDefaultResource("mapred-site.xml");
        return FileSystem.get(URI.create(uri), config);
    }

    public static void CreateFile(Path path, String content) throws IOException {
        FileSystem hdfs = GetFileSystem();
        FSDataOutputStream stream = hdfs.create(path, true);
        stream.write(content.getBytes());
        stream.flush();
        stream.close();
        hdfs.close();
    }
}
