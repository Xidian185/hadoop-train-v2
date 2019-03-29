package com.immoc.bigdata.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

public class HDFSApp {
    public static final String HDFS_PATH = "hdfs://192.168.43.26:8020" ;
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception{
        configuration = new Configuration();
        configuration.set("dfs.replication","1");
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration, "hadoop");
    }

    @After
    public void tearDown(){
        configuration = null;
        fileSystem = null;
        System.out.println("tear down...");
    }

    /**
     * 创建hdfs文件夹
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception{
        Path path = new Path("/hdfsapi/test1");
        boolean result = fileSystem.mkdirs(path);
        System.out.println("resuslt is " + result);
    }

    /**
     * 查看hdfs内容
     */
    @Test
    public void text() throws Exception{
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/cdh_version.properties"));
        IOUtils.copyBytes(fsDataInputStream, System.out, 1024);
    }

    /**
     * 写内容到hdfs
     */
    @Test
    public void write() throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/test3.txt"));
        fsDataOutputStream.writeUTF("hello world");
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }
    /**
     * 副本使用
     */
    @Test
    public void testReplication(){
        System.out.println(configuration.get("dfs.replication"));
    }
    /**
     * 文件名更改
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/test.txt");
        Path newPath = new Path("/hdfsapi/test/test4.txt");
        boolean result = fileSystem.rename(oldPath,newPath);
        System.out.println("重命名结果："+result);
    }
    /**
     * 从本地拷贝文件到hdfs
     */
    @Test
    public void copyFromLocal() throws Exception{
        Path src = new Path("E:\\BaiduNetdiskDownload\\员工离职预测.pdf");
        Path dst = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(src,dst);
    }
    /**
     * 从本地拷贝大文件到hdfs，带进度显示
     */
    @Test
    public void copyBigFileFromLocal() throws Exception{
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/jiaocheng"),
                new Progressable() {
                    @Override
                    public void progress() {
                        System.out.print(".");
                    }
                });
        InputStream inputStream = new BufferedInputStream(new FileInputStream(new File("E:\\BaiduNetdiskDownload\\3、通过JD推荐和亚马逊图书推荐剖析推荐系统功能及核心点：相似度.rar")));
        IOUtils.copyBytes(inputStream,fsDataOutputStream,4096);
    }
    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlockStatus() throws Exception{
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfsapi/jiaocheng"));
        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus,0,fileStatus.getLen());
        for (BlockLocation block : blocks){
            for (String name : block.getNames()){
                System.out.println(name + ":" + block.getOffset() + ":" +block.getLength() + ":" + block.getHosts());
            }
        }
    }

    public static void main(String[] args) {

    }
}
