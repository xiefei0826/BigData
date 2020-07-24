package org.example.hdfs;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;
import org.example.Tools;

import java.io.*;

public class FileExample {
    private static String ResourcePath;
    private static String _testFileName = "test";

    private static Logger logger = Logger.getLogger(FileExample.class);

    public static void main(String[] args) throws IOException {
        logger.info("start");
        File file = File.createTempFile(_testFileName, "");
        ResourcePath = file.getParent() + "/";
        _testFileName = file.getName();
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write("this tmp file");
        writer.flush();
        writer.close();
        logger.info("resource path : " + ResourcePath);
        logger.info("upLoad file start");
        UpLoadFile();
        logger.info("upLoad file end");
        logger.info("create file start");
        CreateFile();
        logger.info("create file end");
        logger.info("create directory start");
        CreateDir();
        logger.info("create directory end");
        logger.info("rename file start");
        RenameFile();
        logger.info("rename file end");
        logger.info("delete file start");
        DeleteFile();
        logger.info("delete file end");
        logger.info("read file start");
        ReadFile();
        logger.info("read file end");
        logger.info("file exists status start");
        isFileExists();
        logger.info("file exists status end");
        logger.info("get update info start");
        GetModificationTime();
        logger.info("get update info end");

        logger.info("get file location start");
        FileLocation();
        logger.info("get file location end");
        logger.info("node list start");
        NodeList();
        logger.info("node list end");

        System.out.println("end");
    }

    private static void UpLoadFile() throws IOException {
        FileSystem dhfs = Tools.GetFileSystem();
        Path src = new Path(ResourcePath + _testFileName);
        Path dst = new Path("/");
        logger.info("upload file begin file list : ");
        ListFileStatus(dst);
        //upload file
        dhfs.copyFromLocalFile(src, dst);
        logger.info("upload file end file list");
        ListFileStatus(dst);
        dhfs.close();
    }

    private static void CreateFile() throws IOException {
        byte[] buff = "Hello Hadoop ".getBytes();
        FileSystem hdfs = Tools.GetFileSystem();
        Path dfs = new Path("/testcreate");
        FSDataOutputStream outputStream = hdfs.create(dfs, true);
        outputStream.write(buff);
        outputStream.close();
        hdfs.close();
    }

    private static void CreateDir() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        Path dfs = new Path("/TestDir");
        hdfs.mkdirs(dfs);
        hdfs.close();
    }

    private static void RenameFile() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        Path srcPath = new Path("/testcreate");
        Path tagPath = new Path("/tagnewname");

        boolean isRename = hdfs.rename(srcPath, tagPath);
        logger.info("file rename status : " + isRename);
        ListFileStatus(new Path("/"));
        hdfs.close();
    }

    private static void DeleteFile() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        Path delFile = new Path("/tagnewname");
        boolean isDeleted = hdfs.delete(delFile, false);
        logger.info("delete file status : " + isDeleted);
        hdfs.close();
    }

    private static void ReadFile() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        FSDataInputStream inputStream = hdfs.open(new Path("/java_error_in_IDEA_79622.log"));
        IOUtils.copyBytes(inputStream, System.out, 1024, false);
        IOUtils.closeStream(inputStream);
        hdfs.close();
    }

    private static void isFileExists() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        Path findFile = new Path("/test");
        boolean isExists = hdfs.exists(findFile);
        logger.info("file Exist : " + isExists);
        hdfs.close();
    }

    private static void GetModificationTime() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        Path filePath = new Path("/" + _testFileName);
        FileStatus fileStatus = hdfs.getFileStatus(filePath);
        long updateTime = fileStatus.getModificationTime();
        logger.info("update time : " + updateTime);
        hdfs.close();
    }

    private static void FileLocation() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        Path filePath = new Path("/" + _testFileName);
        FileStatus fileStatus = hdfs.getFileStatus(filePath);
        BlockLocation[] blockLocations = hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        int blockLen = blockLocations.length;
        for (int i = 0; i < blockLen; i++) {
            String[] hosts = blockLocations[i].getHosts();
            logger.info("block_" + i + "_location : " + hosts[0]);
        }
        hdfs.close();
    }

    private static void NodeList() throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        DistributedFileSystem dhdfs = (DistributedFileSystem) hdfs;
        DatanodeInfo[] datanodeInfos = dhdfs.getDataNodeStats();
        for (int i = 0; i < datanodeInfos.length; i++) {
            logger.info("DataNode_" + i + "_Name : " + datanodeInfos[i].getHostName());
        }
        hdfs.close();
    }

    private static FileStatus[] ListFileStatus(Path path) throws IOException {
        FileSystem hdfs = Tools.GetFileSystem();
        FileStatus[] statuses = hdfs.listStatus(path);
        for (FileStatus status : statuses) {
            logger.info("file path : " + status.getPath());
        }
        return statuses;
    }
}
