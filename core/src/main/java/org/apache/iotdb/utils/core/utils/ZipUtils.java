package org.apache.iotdb.utils.core.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.*;

@Slf4j
public class ZipUtils {
    private static final int BUFSIZE = 1024;

    private static void compressbyType(File src, ZipOutputStream zos,
                                       String baseDir) {
        if (!src.exists())
            return;
        if (src.isFile()) {
            compressFile(src, zos, baseDir);
        } else if (src.isDirectory()) {
            compressDir(src, zos, baseDir);
        }
    }


    public static void compress(String srcFilePath, String destFilePath) {
        File src = new File(srcFilePath);
        if (!src.exists()) {
            throw new RuntimeException(srcFilePath + "不存在");
        }
        File zipFile = new File(destFilePath);
        try {
            FileOutputStream fos = new FileOutputStream(zipFile);
            CheckedOutputStream cos = new CheckedOutputStream(fos, new CRC32());
            ZipOutputStream zos = new ZipOutputStream(cos);
            String baseDir = "";
            compressbyType(src, zos, baseDir);
            zos.close();
        } catch (Exception e) {
            log.error("异常信息:",e);
        }
    }

    private static void compressFile(File file, ZipOutputStream zos,
                                     String baseDir) {
        if (!file.exists())
            return;
        try {
            BufferedInputStream bis = new BufferedInputStream(
                    new FileInputStream(file));
            ZipEntry entry = new ZipEntry(baseDir + file.getName());
            zos.putNextEntry(entry);
            int count;
            byte[] buf = new byte[BUFSIZE];
            while ((count = bis.read(buf)) != -1) {
                zos.write(buf, 0, count);
            }
            bis.close();
        } catch (Exception e) {
            // TODO: handle exception
        }
    }


    private static void compressDir(File dir, ZipOutputStream zos,
                                    String baseDir) {
        if (!dir.exists())
            return;
        File[] files = dir.listFiles((dir1, name) -> {
            if (name.toLowerCase().endsWith(".zip")) {
                return false;
            }
            return true;
        });
        if (files.length == 0) {
            try {
                zos.putNextEntry(new ZipEntry(baseDir + dir.getName()
                        + File.separator));
            } catch (IOException e) {
                log.error("异常信息:",e);
            }
        }
        for (File file : files) {
            compressbyType(file, zos, baseDir + dir.getName() + File.separator);
        }
    }

    public static void decompress(String zipFilePath, String outputDirectory) {
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            Enumeration entries = zipFile.entries();
            ZipEntry zipEntry = null;
            createDirectory(outputDirectory, "");
            while (entries.hasMoreElements()) {
                zipEntry = (ZipEntry) entries.nextElement();
                if (zipEntry.isDirectory()) {
                    String name = zipEntry.getName().trim();
                    createDirectory(outputDirectory, name.substring(0, name.length() - 1));
                } else {
                    String fileName = zipEntry.getName();
                    fileName = fileName.replaceAll("\\\\", "/");
                    if (fileName.contains("/")) {
                        createDirectory(outputDirectory, fileName.substring(0,
                                fileName.lastIndexOf("/")));
                    }
                    File f = new File(outputDirectory + File.separator
                            + zipEntry.getName());
                    f.createNewFile();
                    InputStream in = zipFile.getInputStream(zipEntry);
                    FileOutputStream out = new FileOutputStream(f);
                    byte[] bytes = new byte[BUFSIZE];
                    int len;
                    while ((len = in.read(bytes)) != -1) {
                        out.write(bytes, 0, len);
                    }
                    in.close();
                    out.close();
                }
            }
            zipFile.close();
        } catch (Exception e) {
            log.error("异常信息:",e);
        }
    }

    private static void createDirectory(String directory, String subDirectory) {
        File f = new File(directory + File.separator + subDirectory);
        if (!f.exists())
            f.mkdirs();
    }


    /**
     * pipeline 使用
     *
     * @param srcPath
     */
    public static List<InputStream> decompressToInputStreams(String srcPath,String type) {
        try {
            ZipFile zipFile = new ZipFile(srcPath+"dump.zip");
            Enumeration entries = zipFile.entries();
            ZipEntry zipEntry = null;
            List<InputStream> inputStreams = new ArrayList<>();
            while (entries.hasMoreElements()) {
                zipEntry = (ZipEntry) entries.nextElement();
                if(zipEntry.getName().toLowerCase().endsWith(type)){
                    InputStream in = zipFile.getInputStream(zipEntry);
                    inputStreams.add(in);
                }
            }
            //zipFile.close();
            return inputStreams;
        } catch (Exception e) {
            log.error("异常信息:",e);
        }
        return null;
    }
}
