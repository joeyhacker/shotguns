package com.joey.shotguns;

import com.joey.shotguns.job.JobDescription;
import com.joey.shotguns.utils.HdfsUtil;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;


public class AppSubmitter {

    private static Logger logger = LoggerFactory.getLogger(AppSubmitter.class);

    private static final String APP_MASTER_JAR_NAME = "AppMaster.jar";

    private static final String USER_APP_BASE = "/tmp/apps";

    private static final String SHARE_LIBS_BASE = "/tmp/share/libs";

    private static Path APP_MASTER_IN_HDFS = new Path("/tmp", APP_MASTER_JAR_NAME);

    private YarnConfiguration conf;

    private HdfsUtil hdfsUtil;

    private static AppSubmitter appSubmitter;

    public static AppSubmitter getInstance() {
        if (appSubmitter == null) {
            synchronized (AppSubmitter.class) {
                if (appSubmitter == null) {
                    appSubmitter = new AppSubmitter();
                }
            }
        }
        return appSubmitter;
    }

    private AppSubmitter() {
        try {
            conf = new YarnConfiguration();
            String hadoop_conf_dir = System.getenv("HADOOP_CONF_DIR");
            if (StringUtils.isNotBlank(hadoop_conf_dir)) {
                logger.info("HADOOP_CONF_DIR using {}", hadoop_conf_dir);
                conf.addResource(new Path(hadoop_conf_dir, "core-site.xml"));
                conf.addResource(new Path(hadoop_conf_dir, "hdfs-site.xml"));
                conf.addResource(new Path(hadoop_conf_dir, "yarn-site.xml"));
            } else {
                logger.info("HADOOP_HOME not config, using DEFAULT");
            }
            hdfsUtil = new HdfsUtil(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private URL path2Url(Path path) {
        URI uri = path.toUri();
        String pathStr = path.toString();
        int idx = path.toString().indexOf("/", 7);
        return URL.newInstance(uri.getScheme(), uri.getHost(), uri.getPort(), pathStr.substring(idx));
    }

    private void setupAppMasterJar(LocalResource appMasterJar) throws Throwable {
        FileStatus fileStatus = hdfsUtil.status(APP_MASTER_IN_HDFS);
        String localPath = System.getenv("MASTER_JAR_PATH");
        if (localPath == null) {
            localPath = AppMaster.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        }
        File localFile = new File(localPath);
        if (fileStatus == null || localFile.lastModified() != fileStatus.getModificationTime() || localFile.length() != fileStatus.getLen()) {
            logger.info("uploading AppMaster jar file, local {}", localPath);
            hdfsUtil.upload(localPath, APP_MASTER_IN_HDFS);
            fileStatus = hdfsUtil.status(APP_MASTER_IN_HDFS);
            logger.info("AppMaster jar file update finished.");
        }
        appMasterJar.setResource(path2Url(fileStatus.getPath()));
        appMasterJar.setSize(fileStatus.getLen());
        appMasterJar.setTimestamp(fileStatus.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
        appMasterJar.setShouldBeUploadedToSharedCache(true);
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv, YarnConfiguration conf, JobDescription job) {
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);
            System.err.println("set " + ApplicationConstants.Environment.CLASSPATH.name() + " = " + c.trim());
        }

        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*", File.pathSeparator);

        System.err.println("set " + ApplicationConstants.Environment.CLASSPATH.name() + " = " + ApplicationConstants.Environment.PWD.$() + File.separator + "*");

        String appId = UUID.randomUUID().toString();
        List<String> classList = new ArrayList();
        try {
            Path targetPath = new Path(USER_APP_BASE, appId);
            Path targetJar = new Path(targetPath, "bootstrap.jar");
            Path targetLibs = new Path(targetPath, "libs");
            hdfsUtil.mkdirs(targetPath, targetLibs);
            hdfsUtil.upload(job.getAppJar(), targetJar);
            classList.add(targetJar.toString());
            for (String dir : job.getDependencies()) {
                File fs = new File(dir);
                for (File file : fs.listFiles()) {
                    Path p = uploadLib(file.getPath(), targetLibs);
                    classList.add("libs/" + file.getName());
                }
            }
            logger.info("APP CLASSPATH = {}", StringUtils.join(classList, ":"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        Apps.addToEnvironment(appMasterEnv, "Containers", String.valueOf(job.getContainers()), File.pathSeparator);
        Apps.addToEnvironment(appMasterEnv, "Memory", String.valueOf(job.getRequirement().getMemory()), File.pathSeparator);
        Apps.addToEnvironment(appMasterEnv, "VirtualCores", String.valueOf(job.getRequirement().getVirtualCores()), File.pathSeparator);
        Apps.addToEnvironment(appMasterEnv, "MainClass", String.valueOf(job.getMainClass()), File.pathSeparator);
        Apps.addToEnvironment(appMasterEnv, "AppClassPath", StringUtils.join(classList, ":"), File.pathSeparator);
        Apps.addToEnvironment(appMasterEnv, "AppDir", USER_APP_BASE + "/" + appId, File.pathSeparator);
    }

    private Path uploadLib(String localPath, Path targetLibs) {
        int idx = localPath.lastIndexOf("/");
        String jarName = localPath.substring(idx + 1);
        try {
            Path sharePath = new Path(SHARE_LIBS_BASE, jarName);
            File file = new File(localPath);
            FileStatus status = hdfsUtil.status(sharePath);
            if (status != null && status.getModificationTime() == file.lastModified() && status.getLen() == file.length()) {
                return sharePath;
            } else {
                Path targetPath = new Path(targetLibs, jarName);
                hdfsUtil.upload(localPath, targetPath);
                return targetPath;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String extractPath(Path path) {
        String pathStr = path.toString();
        int idx = path.toString().indexOf("/", 7);
        return pathStr.substring(idx);
    }


    public void submit(JobDescription job, AppSubmitListener listener) throws Throwable {

        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = yarnClient.createApplication();

        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer =
                Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
                Collections.singletonList(
                        "$JAVA_HOME/bin/java" +
                                " com.joey.shotguns.AppMaster" +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                )
        );

        // Setup jar for ApplicationMaster
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        setupAppMasterJar(appMasterJar);
        amContainer.setLocalResources(
                Collections.singletonMap(APP_MASTER_JAR_NAME, appMasterJar));


        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv, conf, job);
        amContainer.setEnvironment(appMasterEnv);


        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(512);
        capability.setVirtualCores(1);

        // Finally, set-up ApplicationSubmissionContext for the application
        ApplicationSubmissionContext appContext =
                app.getApplicationSubmissionContext();
        appContext.setApplicationName(job.getAppName()); // application name
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        boolean done = false;
        while (!done) {
            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
            YarnApplicationState appState = appReport.getYarnApplicationState();
            if (appState == YarnApplicationState.FINISHED) {
                listener.onFinish(appReport.getFinalApplicationStatus().name(), appReport.getFinishTime());
                done = true;
            } else if (appState == YarnApplicationState.FAILED) {
                listener.onFailure(appReport.getFinalApplicationStatus().name(), appReport.getDiagnostics());
                done = true;
            } else {
                listener.onProgress(appState.name(), appReport.getProgress());
            }
            Thread.sleep(500);
        }
    }

    public static void main(String[] args) throws Throwable {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options();
        options.addOption("f", "file", true, "job description file, use json format");
        CommandLine commandLine = parser.parse(options, args);
        if (commandLine.hasOption("f")) {
            String filePath = commandLine.getOptionValue("f");
            ObjectMapper mapper = new ObjectMapper();
            try {
                JobDescription job = mapper.readValue(new File(filePath), JobDescription.class);
                System.out.println(job);
                AppSubmitter submitter = new AppSubmitter();
                submitter.submit(job, new AppSubmitListener() {
                    @Override
                    public void onProgress(String status, float progress) {
                        logger.info("status {}, progress {}", status, progress);
                    }

                    @Override
                    public void onFinish(String status, long finishTime) {
                        logger.info("status {}, finishTime {}", status, finishTime);
                    }

                    @Override
                    public void onFailure(String status, String diagnostics) {
                        logger.error("status {}, diagnostics {}", status, diagnostics);
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("OptsCommonsCLI", options);
        }
//        System.out.println(commandLine.getOptionValue("f"));
//        String path = "/Users/joey/shotguns/shotguns-core/src/main/resources/job.json";
//        int idx = path.lastIndexOf("/");
//        System.out.println(path.substring(idx));
//        ObjectMapper mapper = new ObjectMapper();
//        try {
//            JobDescription map = mapper.readValue(new File(path), JobDescription.class);
//            System.out.println(map);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
