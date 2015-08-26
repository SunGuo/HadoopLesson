package com.rongxin.hadoop.lesson1;

/**
 * Created by guoxian1 on 15/3/21.
 */

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;


public class hdfsDemo {
    private static hdfsDemo hadoopService;

    private static Log logger = LogFactory.getLog(hdfsDemo.class);

    static Configuration conf;

    private static FileSystem fs;

    private String prefix_fs;

    private hdfsDemo() {
        try {
            conf = new Configuration();
            prefix_fs = "hdfs://localhost:9000";
            conf.set("fs.default.name", "hdfs://localhost:9000");
            URI uri = URI.create("hdfs://localhost:9000");
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e.toString());
            logger.error(e);
        }
    }

    public synchronized static hdfsDemo getInstance() {
        if (hdfsDemo.hadoopService == null || hdfsDemo.fs == null) {
            hadoopService = null;
            hadoopService = new hdfsDemo();
        }
        return hadoopService;
    }

    public void copyToLocal(String dfsPath, String localPath)
            throws IOException {
        fs.copyToLocalFile(false, new Path(dfsPath), new Path(localPath));
    }

    public void createAndWriteString(String path, String content,
                                     String charset, boolean overwrite) throws Exception {
        try {
            DataOutputStream os = fs.create(new Path(path), overwrite);
            os.write(content.getBytes(charset));
            os.flush();
            os.close();
        } catch (Exception e) {
            logger.error(e.toString());
            throw e;
        }
    }

    public FSDataOutputStream createFileAndGetOutputStream(String file)
            throws IOException {
        FSDataOutputStream stm = fs.create(new Path(file), true, fs.getConf()
                .getInt("io.file.buffer.size", 4096));
        return stm;
    }

    public void append(FSDataOutputStream stm, byte[] toWrite) throws Exception {
        try {
            stm.write(toWrite);
            stm.sync();
        } catch (IOException e) {
            logger.error(e.toString(), e);
            throw e;
        }
    }

    public void closeTheOutputStream(FSDataOutputStream stm) {
        try {
            stm.flush();
            stm.close();
        } catch (IOException e) {
            logger.error(e.toString(), e);
        }
    }

    public void appendWriteString(String path, String content, String charset)
            throws Exception {
        try {
            if (!fs.exists(new Path(path))) {
                fs.createNewFile(new Path(path));
            }
            DataOutputStream os = fs.append(new Path(path));
            os.write(content.getBytes(charset));
            os.flush();
            os.close();
        } catch (IOException e) {
            logger.error(e);
            throw e;
        }
    }

    public void uploadFile(File file, String dest) throws IOException {
        this.uploadFile(file, dest, true);
    }

    public boolean uploadFile(File file, String dest, boolean overwrite)
            throws IOException {
        Path inFile = new Path(dest);
        if (!overwrite) {
            if (fs.exists(inFile)) {
                return false;
            }
        } else {
            this.deleteFile(dest);
        }
        FileUtil.copy(file, fs, inFile, false, conf);
        return true;
    }

    public void writeFile(String path, OutputStream outputStream)
            throws IOException {
        Path inFile = new Path(path);
        FSDataInputStream fis = fs.open(inFile);
        byte[] buffer = new byte[1000];
        while (!(fis.read(buffer) == -1)) {
            outputStream.write(buffer);
        }
        fis.close();
    }

    public void deleteFile(String path) throws IOException {
        Path inFile = new Path(path);
        if (fs.exists(inFile)) {
            fs.delete(inFile, false);
        }
    }

    public boolean deleteFilesInSrcPath(String srcPath) {
        try {
            List<Path> paths = getFilesbyPath(srcPath, true);
            for (Path p : paths) {

                deleteFile(p.toString());
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }

    }

    public void deleteDir(String path) throws IOException {
        Path inFile = new Path(path);
        if (fs.exists(inFile)) {
            fs.delete(inFile, true);
        }
    }

    public byte[] getFile(String src) throws IOException {
        Path inFile = new Path(src);
        // fs.getFileStatus(inFile).getModificationTime();
        FSDataInputStream fis = fs.open(inFile);
        byte[] buffer = new byte[fis.available()];
        fis.read(buffer);
        fis.close();
        return buffer;
    }

    public byte[] readFile(String path) throws IOException {
        Path inFile = new Path(path);
        // fs.getFileStatus(inFile).getModificationTime();
        FSDataInputStream fis = fs.open(inFile);
        byte[] buffer = null;
        if (fis.available() > 1 * 1024 * 1024) {
            buffer = new byte[1 * 1024 * 1024];
        } else {
            buffer = new byte[fis.available()];
        }
        fis.read(buffer);
        fis.close();
        return buffer;
    }

    public String readHdfsPath(String path) throws Exception {
        StringBuffer content = new StringBuffer("");
        ;
        if (isFile(path)) {
            content.append(read(path));
        } else if (isTopDir(new Path(path))) {
            List<String> files = listPaths(path);
            Iterator<String> it = files.iterator();
            while (it.hasNext()) {
                String temp = it.next().toString();
                content.append(read(temp));
            }
        }
        return content.toString();
    }

    public String read(String path) throws Exception {
        try {
            if (isFile(path)) {
                Path inFile = new Path(path);
                // reading
                FSDataInputStream dis = fs.open(inFile);
                StringBuffer sb = new StringBuffer("");
                String line = null;
                while ((line = dis.readLine()) != null) {
                    sb.append(line).append("\n");
                }
                dis.close();
                return sb.toString();
            } else {
                throw new Exception();
            }
        } catch (Exception e) {
            throw e;
        }

    }

    public String readlimit(String path) throws Exception {
        try {
            if (isFile(path)) {
                Path inFile = new Path(path);
                // reading
                FSDataInputStream dis = fs.open(inFile);
                StringBuffer sb = new StringBuffer("");
                String line = null;
                int allline = dis.available();
                logger.info("allline:" + dis.available());
                while ((line = dis.readLine()) != null) {
                    if (sb.length() < 2000) {
                        sb.append(line).append("\n");
                    }
                }
                dis.close();

                return sb.toString();
            } else {
                throw new Exception();
            }
        } catch (Exception e) {
            throw e;
        }

    }

    public List<String> listPaths(String path) throws Exception {
        long st = System.currentTimeMillis();
        FileStatus[] statuses = fs.listStatus(new Path(path));
        List<String> files = new ArrayList<String>();
        if (statuses == null)
            return null;

        for (FileStatus status : statuses) {
            path = status.getPath().toString();
            if (path != null && path.trim().startsWith(prefix_fs)) {
                path = path.replace(prefix_fs, "");
            }
            files.add(path);
        }
        logger.info("size    :" + files.size());
        return files;
    }

    public FileSystem getHDFSClient() {
        return fs;
    }

    /**
     * 得到路径下所有文件
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<Path> getFilesbyPath(String path, boolean r) throws Exception {
        List<Path> paths = new ArrayList<Path>();
        Path p = new Path(path);
        if (!fs.exists(p))
            return null;
        FileStatus file = fs.getFileStatus(p);
        if (file.isDir()) {
            FileStatus[] files = fs.listStatus(p);
            for (FileStatus f : files) {
                if (f.isDir() && r) {
                    paths.addAll((List<Path>) getFilesbyPath(f.getPath()
                            .toString(), r));

                } else if (!f.isDir()) {
                    paths.add(f.getPath());
                }
            }
        } else
            paths.add(file.getPath());

        return paths;
    }

    /**
     * 返回路径下文件总大小
     *
     * @param path
     * @return
     * @throws Exception
     */
    public long sumFilesSize(String path) throws Exception {
        Path p = new Path(path);
        long size = 0l;
        if (!fs.exists(p))
            return 0;
        FileStatus file = fs.getFileStatus(p);
        if (file.isDir()) {
            FileStatus[] files = fs.listStatus(p);
            for (FileStatus f : files) {
                if (f.isDir())
                    size += sumFilesSize(f.getPath().toString());
                else
                    size += f.getLen();
            }
        } else
            size += file.getLen();
        return size;
    }

    public long getFileSize(String path) throws Exception {
        Path p = new Path(path);
        if (!fs.exists(p))
            throw new Exception("file is not exists!");
        return fs.getFileStatus(p).getLen();

    }

    public long getBlockSize(String path) throws Exception{

        Path p = new Path(path);
        if (!fs.exists(p))
            throw new Exception("file is not exists!");
        return fs.getContentSummary(p).getLength();
    }
    public long sumLocalFilesSize(String dir) throws Exception {
        long size = 0l;
        File src = new File(dir);
        if (!src.canRead())
            throw new IOException("file cann't be read!"
                    + src.getAbsolutePath());
        if (src.isDirectory()) {
            File[] ff = src.listFiles();
            for (File f : ff) {
                size += f.length();
            }
        }
        return size / 1024;
    }

    public boolean isTopDir(Path path) throws IOException {
        FileStatus[] statuses = fs.listStatus(path);
        for (FileStatus s : statuses) {
            if (s.isDir())
                return false;
        }
        return true;
    }

    public List<Path> findTopDir(Path path) throws IOException {
        List<Path> paths = new ArrayList<Path>();
        if (!fs.exists(path))
            return null;

        if (fs.exists(path) && isTopDir(path)) {
            paths.add(path);
            return paths;
        } else {
            FileStatus[] statuses = fs.listStatus(path);

            for (FileStatus s : statuses) {
                Path temp = s.getPath();
                // System.out.println(temp.toString()+"-"+isTopDir(temp));
                if (isTopDir(temp))
                    paths.add(temp);
                else {
                    paths.addAll(findTopDir(temp));
                }
            }
        }
        return paths;
    }

    public void copyFilesOfLocalDir(File src, Path dst) throws IOException {
        if (!src.canRead())
            throw new IOException("file cann't be read!"
                    + src.getAbsolutePath());
        if (src.isDirectory()) {
            File[] ff = src.listFiles();
            for (File f : ff) {
                fs.copyFromLocalFile(new Path(f.getAbsolutePath()), dst);
            }
        } else {
            fs.copyFromLocalFile(new Path(src.getAbsolutePath()), dst);
        }
    }

    public void copyOnHDFS(String src, String dst) throws Exception {
        Path p1 = new Path(src);
        Path p2 = new Path(dst);
        FileUtil.copy(fs, p1, fs, p2, false, conf);
    }

    /**
     * 拷贝srcpath下所有文件到dstpath
     *
     * @param srcPath
     * @param dst
     * @return
     */
    public boolean copyFilesInSrcPathOnHDFS(String srcPath, String dst) {
        try {
            List<Path> paths = getFilesbyPath(srcPath, true);
            boolean dstdir = false;
            if (!dst.endsWith(File.separator))
                dst += File.separator;
            if (fs.getFileStatus(new Path(dst)).isDir())
                dstdir = true;
            for (Path p : paths) {
                if (dstdir && fs.exists(new Path(dst + p.getName())))
                    copyOnHDFS(
                            p.toString(),
                            dst + p.getName() + "_"
                                    + System.currentTimeMillis());
                else {
                    copyOnHDFS(p.toString(), dst);
                }
            }
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    /**
     * 根据传入的fileNames比对path下的文件变化
     *
     * @param path
     * @param fileNames
     * @return
     */
    public List detectChangedFiles(String path, List<String> fileNames,
                                   List<Long> lastModifyTime) {
        if (fileNames == null)
            fileNames = new ArrayList<String>();
        if (lastModifyTime == null)
            lastModifyTime = new ArrayList<Long>();

        List<String> modifiedFiles = new ArrayList<String>();

        try {
            Path f = new Path(path); // 需要检查的目录
            List<Path> files = this.getFilesbyPath(path, true);
            System.out.println(files.size());
            int index = 0;
            for (Path file : files) {
                index = fileNames.indexOf(file.toString());
                if (index != -1) {

                    if (fs.getFileStatus(file).getModificationTime() != lastModifyTime
                            .get(index).longValue()) {
                        modifiedFiles.add(fileNames.get(index));
                    }

                } else {
                    fileNames.add(file.toString());
                    lastModifyTime.add(new Long(fs.getFileStatus(file)
                            .getModificationTime()));
                    modifiedFiles.add(file.toString());
                }
            }
            return modifiedFiles;
        } catch (Exception ex) {
            ex.printStackTrace();
            return null;
        }

    }

    public void copyFilesOfLocalDir(String src, Path dst) throws IOException {
        copyFilesOfLocalDir(new File(src), dst);
    }

    public void copyFilesOfLocalDir(String src, String dst) throws IOException {
        Path p = new Path(dst);
        copyFilesOfLocalDir(src, p);
    }

    public boolean checkIsSamePath(String path1, String path2) {
        Path p1 = new Path(path1);
        Path p2 = new Path(path2);
        return p1.equals(p2);
    }

    public String convertPath(String path) throws IOException {

        Path p = new Path(path);
        if (!fs.exists(p))
            return null;
        FileStatus status = fs.getFileStatus(p);

        if (fs.exists(p) && status.isDir()) {
            return status.getPath().toString();
        }
        return null;
    }

    public boolean mkDir(String path) throws IOException {
        Path inFile = new Path(path);

        if (fs.exists(inFile))
            return true;
        if (!fs.exists(inFile)) {
            return fs.mkdirs(inFile);
        }
        return false;
    }

    public boolean exists(String path) {
        Path inFile = new Path(path);
        try {
            if (fs.exists(inFile))
                return true;
        } catch (Exception e) {
            logger.error(e);
            return false;
        }
        return false;
    }

    public boolean isFile(String path) throws Exception {
        long st = System.currentTimeMillis();
        Path inFile = new Path(path);
        boolean flag = fs.isFile(inFile);
        // System.out.println("isFile time(ms) "+(System.currentTimeMillis()-st));
        return flag;
    }

    public void fullDelete(String path) throws IOException {
        FileUtil.fullyDelete(new File(path));
    }

    public void appendWriteTextFile(String p, List<String> files,
                                    boolean compress) throws IOException {
        FileSystem fs = getHDFSClient();
        Path path = new Path(p);
        DataOutputStream os = null;
        try {
            if (!fs.exists(path)) {
                fs.createNewFile(path);
            }
            os = fs.append(path);
            for (String f : files) {
                os.write(f.getBytes());
            }
        } finally {
            if (os != null) {
                os.flush();
                os.close();
            }
        }
    }

    /**
     * 使用sequencefile合并文件,compress 为lzo压缩。
     * isNullKey为true则使用NullWritable为key,否则使用文件名为key
     *
     * @param p
     * @param files
     * @param compress
     * @param isNullKey
     * @throws IOException
     */
    public void writeSequceFile(String p, List<String> files, boolean compress,
                                boolean isNullKey) throws IOException {
        FileSystem fs = getHDFSClient();
        Path path = new Path(p);

        SequenceFile.Writer writer = null;
        Writable key = null, value = null;
        byte[] buffer = new byte[0];
        try {
            if (compress) {
                if (!isNullKey) {
                    writer = SequenceFile
                            .createWriter(
                                    fs,
                                    fs.getConf(),
                                    path,
                                    Text.class,
                                    BytesWritable.class,
                                    CompressionType.BLOCK,
                                    (CompressionCodec) ReflectionUtils.newInstance(
                                            Class.forName("com.hadoop.compression.lzo.LzoCodec"),
                                            conf));
                } else {
                    writer = SequenceFile
                            .createWriter(
                                    fs,
                                    fs.getConf(),
                                    path,
                                    NullWritable.class,
                                    BytesWritable.class,
                                    CompressionType.BLOCK,
                                    (CompressionCodec) ReflectionUtils.newInstance(
                                            Class.forName("com.hadoop.compression.lzo.LzoCodec"),
                                            conf));
                }
                for (String f : files) {
                    if (!isNullKey) {
                        key = new Text();
                        ((Text) key).set(f);
                    } else {
                        key = NullWritable.get();
                    }
                    value = new BytesWritable();

                    buffer = hdfsDemo.getInstance().getFile(f);
                    ((BytesWritable) value).set(hdfsDemo.getInstance()
                            .getFile(f), 0, buffer.length);
                    writer.append(key, value);
                }
            } else {
                if (!isNullKey) {
                    key = new Text();
                    writer = SequenceFile.createWriter(fs, fs.getConf(), path,
                            Text.class, Text.class);
                } else {
                    writer = SequenceFile.createWriter(fs, fs.getConf(), path,
                            NullWritable.class, Text.class);
                }

                for (String f : files) {
                    if (!isNullKey) {
                        key = new Text();
                        ((Text) key).set(f);
                    } else {
                        key = NullWritable.get();
                    }
                    value = new Text();

                    buffer = hdfsDemo.getInstance().getFile(f);
                    ((Text) value).set(buffer);
                    writer.append(key, value);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public void readSeqFile(String seqfile) throws IOException {
        Path path = new Path(seqfile);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, fs.getConf());
            Writable key = (Writable) ReflectionUtils.newInstance(
                    reader.getKeyClass(), fs.getConf());
            Writable value = (Writable) ReflectionUtils.newInstance(
                    reader.getValueClass(), fs.getConf());
            long position = reader.getPosition();
            int index = 0;
            while (reader.next(key, value)) {
                System.out.println(key);
                // String syncSeen = reader.syncSeen() ? "*" : "";
                // System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
                // key, value);
                // position = reader.getPosition(); // beginning of next record
                index++;
            }
            System.out.println("total " + index);
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    public InputStream getInputStreamByFilePath(String filepath) {
        try {
            Path path = new Path(filepath);
            if (!fs.exists(path))
                return null;
            FSDataInputStream dis = fs.open(path);
            return dis;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public void listFileNamesFromSeqFile(String seqfile) throws IOException {
        Path path = new Path(seqfile);
        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, fs.getConf());
            Writable key = (Writable) ReflectionUtils.newInstance(
                    reader.getKeyClass(), fs.getConf());
            Writable value = (Writable) ReflectionUtils.newInstance(
                    reader.getValueClass(), fs.getConf());
            long position = reader.getPosition();
            int index = 0;
            while (reader.next(key, value)) {
                System.out.println(key);
                // String syncSeen = reader.syncSeen() ? "*" : "";
                // System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen,
                // key, value);
                // position = reader.getPosition(); // beginning of next record
                index++;
            }
            System.out.println("total " + index);
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    public long getDefaultBlockSize() {
        Long defaultsize = null;
        if (defaultsize == null)
            defaultsize = fs.getDefaultBlockSize();
        return defaultsize;
    }

    private void monitorPathChanges() {
        try {
            List<String> srcFiles = new ArrayList<String>();
            List<Long> lastModifyTime = new ArrayList<Long>();
            while (true) {
                List<String> changedFiles = hdfsDemo
                        .getInstance()
                        .detectChangedFiles(
                                "/user/hdfs/rawlog/traffic_traffic8y65w0528uk7a_test/2011_11_11/19",
                                srcFiles, lastModifyTime);
                System.out.println("==========changed files==============");
                for (String file : changedFiles)
                    System.out.println(file);

                Thread.sleep(60 * 1000);

            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
    }



    // trash path:/user/hdfs/.Trash/Current/user/hdfs/extendlog
    // path: /user/hdfs/extendlog
    /**
     * 从srcpath路径下所有数据及结构mv到destpath下
     */
    public void mvFromSrcPathToDestPath(String srcpath, String destpath) {
        try {
            List<Path> files = getFilesbyPath(srcpath, true);
            String dir = null;
            Path dirP = null;
            Path newPath = null;
            for (Path path : files) {
                dir = path.getParent().toString().replace(srcpath, destpath);
                dirP = new Path(dir);
                if (!fs.exists(dirP))
                    fs.mkdirs(dirP);
                newPath = new Path(path.getName(), dirP);
                fs.rename(path, newPath);
                // logger.info("from "+path.toString()+" to "+newPath.toString());

            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
    }

    public void mvFile(String srcFile, String destDir) {
        try {
            Path destD = new Path(destDir);
            Path srcP = new Path(srcFile);
            if (!fs.exists(destD))
                fs.mkdirs(destD);
            Path newPath = new Path(srcP.getName(), destD);
            // System.out.println(srcP.toString()+"->"+newPath.toString());
            fs.rename(srcP, newPath);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] arg) {

        try {

            hdfsDemo hdfs = hdfsDemo.getInstance();

            // System.out.println();
            // long st = System.currentTimeMillis();
            /*
            List<String> pp = hdfsDemo.getInstance().listPaths("/user/imeda/zhaoguoxian");
            for(String l : pp){
                System.out.println(l);
            }
            System.out.println(hdfsDemo.getInstance().getFileSize("/user/imeda/zhaoguoxian"));
            System.out.println(hdfsDemo.getInstance().getBlockSize("/user/imeda/zhaoguoxian"));
            */
            // System.out.println("copy time(s):"
            // + (System.currentTimeMillis() - st) / 1000);
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.toString());
        }
    }
}

