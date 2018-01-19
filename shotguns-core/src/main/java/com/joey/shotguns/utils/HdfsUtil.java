package com.joey.shotguns.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.util.ConverterUtils;

public class HdfsUtil {

    private Configuration config;

    public HdfsUtil(Configuration config) {
        this.config = config;
    }

    public void mkdirs(Path... targetPaths) throws Exception {
        FileSystem fs = FileSystem.get(config);
        for (Path p : targetPaths)
            fs.mkdirs(p);
    }

    public boolean clean(Path targetPath) throws Exception {
        FileSystem fs = FileSystem.get(config);
        if (fs.exists(targetPath)) {
            boolean isDeleted = fs.delete(targetPath, true);
            if (isDeleted) {
                return fs.mkdirs(targetPath);
            }
        }
        return false;
    }

    public void upload(String filePath, Path targetPath) throws Exception {
        FileSystem fs = FileSystem.get(config);
        fs.copyFromLocalFile(false, true, new Path(filePath), targetPath);
    }

    public boolean exist(Path targetPath) throws Exception {
        FileSystem fs = FileSystem.get(config);
        return fs.exists(targetPath);
    }

    public FileStatus status(Path targetPath) throws Exception {
        if (exist(targetPath)) {
            FileSystem fs = FileSystem.get(config);
            return fs.getFileStatus(targetPath);
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String hadoop_home = "/Users/joey/tmp/hadoop1";
        System.setProperty("HADOOP_CONF_DIR", hadoop_home);
        conf.addResource(new Path(hadoop_home, "core-site.xml"));
        conf.addResource(new Path(hadoop_home, "hdfs-site.xml"));
        conf.addResource(new Path(hadoop_home, "yarn-site.xml"));

        String APP_MASTER_JAR_NAME = "AppMaster.jar";
        Path APP_MASTER_IN_HDFS = new Path("tmp", APP_MASTER_JAR_NAME);

        System.out.println("isAbsolute = " + APP_MASTER_IN_HDFS.isAbsolute());

        URL fileUrl = ConverterUtils.getYarnUrlFromPath(
                FileContext.getFileContext().makeQualified(APP_MASTER_IN_HDFS));


        System.out.println(fileUrl);

//        HdfsUtil hdfsUtil = new HdfsUtil(conf);
//        hdfsUtil.upload();
//        FileStatus fileStatus = hdfsUtil.status(APP_MASTER_IN_HDFS);
//        String path = fileStatus.getPath().toString();
//        System.out.println(path);
//        int idx = path.indexOf("/", 7);
//        System.out.println(path.substring(idx));
//        System.out.println(fileStatus.getPath().toString());
//        System.setProperty("HADOOP_CONF_DIR", "/Users/joey/tmp/hadoop");
//        Path path = new Path("/tmp/athenax/libs/activation-1.1.jar");
//        FileSystem fs = FileSystem.get(loadConf());
//        System.out.println(fs.getFileStatus(path).getPath().toUri());
//        System.out.println(fs.getUri());
//        System.out.println(path.);
//        try {
//            boolean ret =  getInstance().clean("/tmp/wyk_test");
//            System.out.println(ret);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }
}
