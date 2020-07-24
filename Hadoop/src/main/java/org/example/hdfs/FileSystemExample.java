package org.example.hdfs;

import org.apache.hadoop.fs.*;
import org.example.Tools;

import java.io.IOException;


public class FileSystemExample {

    private static Path path = new Path("/example");
    private static String fileName = "test";

    public static void main(String[] args) throws IOException {

        CreateDirectory();
        ListDirectory();
        DeleteDirectory();
        ListDirectory();
        CreateDirectory();
        CreateFile();
        ListDirectory();
        WriteFile();
        ReadFile();
        DeleteFile();
        ListDirectory();
    }

    /**
     * 创建目录
     *
     * @throws IOException
     */

    private static void CreateDirectory() throws IOException {
        FileSystem fs = Tools.GetFileSystem();
        if (!fs.exists(path))
            fs.mkdirs(path);

        FileStatus status = fs.getFileStatus(path);
        System.out.println(status);
        fs.close();
    }

    /**
     * 列表目录及当前子目录
     *
     * @throws IOException
     */
    private static void ListDirectory() throws IOException {
        FileSystem fs = Tools.GetFileSystem();
        if (fs.exists(path)) {
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (fileStatuses.length == 0)
                System.out.println(path + " nothing!");
            for (FileStatus fileStatus : fileStatuses) {
                System.out.println(fileStatus);
            }
        } else
            System.out.println(path + " does not exist!");
    }

    /**
     * 删除目录指定
     *
     * @throws IOException
     */
    private static void DeleteDirectory() throws IOException {
        FileSystem fs = Tools.GetFileSystem();
        //这里用递归删除，目前下有子文件也会被删除。
        boolean deleteFlag = fs.delete(path, true);
        System.out.println("delete flag : " + deleteFlag);
        fs.close();
    }


    private static void CreateFile() throws IOException {
        Path filePath = Path.mergePaths(path, new Path("/" + fileName));
        FileSystem fs = Tools.GetFileSystem();
        System.out.println(filePath);
        // 如果文件存在则不会创建
        boolean createFlag = fs.createNewFile(filePath);
        System.out.println(createFlag);
        fs.close();

    }

    private static void WriteFile() throws IOException {
        Path filePath = Path.mergePaths(path, new Path("/" + fileName));
        FileSystem fs = Tools.GetFileSystem();
        FSDataOutputStream fsDataOutputStream = fs.append(filePath);
        fsDataOutputStream.write("asdf".getBytes());
        fsDataOutputStream.close();
        fs.close();
    }

    private static void ReadFile() throws IOException {
        Path filePath = Path.mergePaths(path, new Path("/" + fileName));
        FileSystem fs = Tools.GetFileSystem();
        FSDataInputStream fsDataInputStream = fs.open(filePath);

        byte[] data = new byte[1024];
        StringBuffer sb = new StringBuffer();
        while (fsDataInputStream.read(data) != -1) {
            sb.append(new String(data));
        }
        System.out.println(sb.toString());
        fsDataInputStream.close();
        fs.close();

    }

    private static void DeleteFile() throws IOException {
        Path filePath = Path.mergePaths(path, new Path("/" + fileName));
        FileSystem fs = Tools.GetFileSystem();

        fs.deleteOnExit(filePath);
        fs.close();
    }


}
