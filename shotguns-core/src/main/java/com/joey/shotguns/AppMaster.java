package com.joey.shotguns;

import com.joey.shotguns.utils.HdfsUtil;
import com.joey.shotguns.utils.NetUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AppMaster {

    // Initialize clients to ResourceManager and NodeManagers
    static Configuration conf = new YarnConfiguration();
    static HdfsUtil hdfsUtil = new HdfsUtil(conf);

    public static void main(String[] args) throws Exception {

        final String mainClass = System.getenv("MainClass");
        final String appClassPath = System.getenv("AppClassPath");
        final int containers = Integer.valueOf(System.getenv("Containers"));
        final int memory = Integer.valueOf(System.getenv("Memory"));
        final int virtualCores = Integer.valueOf(System.getenv("VirtualCores"));
        final String appDir = System.getenv("AppDir");

        AMRMClient<AMRMClient.ContainerRequest> rmClient = AMRMClient.createAMRMClient();
        rmClient.init(conf);
        rmClient.start();

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        // Register with ResourceManager
        rmClient.registerApplicationMaster(NetUtils.getHostName(), 0, "http://xxx/xxx");

        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(memory);
        capability.setVirtualCores(virtualCores);

        // Make container requests to ResourceManager
        for (int i = 0; i < containers; ++i) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            rmClient.addContainerRequest(containerAsk);
        }

        // Obtain allocated containers, launch and check for responses
        int responseId = 0;
        int completedContainers = 0;
        while (completedContainers < containers) {
            AllocateResponse response = rmClient.allocate(responseId++);
            for (Container container : response.getAllocatedContainers()) {
                // Launch container by create ContainerLaunchContext
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(
                        "$JAVA_HOME/bin/java" +
//                                " -classpath" +
//                                " " + appClassPath +
                                " " + mainClass +
                                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                ));

                // set classpath for app
                Map<String, String> appEnv = new HashMap<String, String>();
                Map<String, LocalResource> localResources = new HashMap<>();
                setupAppEnv(appEnv, localResources, appClassPath, appDir);
                ctx.setEnvironment(appEnv);
                ctx.setLocalResources(localResources);

                nmClient.startContainer(container, ctx);
            }
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                ++completedContainers;
            }
            Thread.sleep(500);
        }

        // Un-register with ResourceManager
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
    }


    private static URL path2Url(Path path) {
        URI uri = path.toUri();
        String pathStr = path.toString();
        int idx = path.toString().indexOf("/", 7);
        return URL.newInstance(uri.getScheme(), uri.getHost(), uri.getPort(), pathStr.substring(idx));
    }

    private static void setupAppEnv(Map<String, String> appEnv, Map<String, LocalResource> localResources, String appClassPath, String appDir) {
        for (String cp : appClassPath.split(":")) {
            int idx = cp.lastIndexOf("/");
            String fileName = cp.substring(idx + 1);
            try {
                FileStatus fileStatus = hdfsUtil.status(new Path(appDir, cp));
                LocalResource resource = Records.newRecord(LocalResource.class);
                resource.setResource(path2Url(fileStatus.getPath()));
                resource.setType(LocalResourceType.FILE);
                resource.setVisibility(LocalResourceVisibility.PUBLIC);
                resource.setSize(fileStatus.getLen());
                resource.setTimestamp(fileStatus.getModificationTime());
                resource.setShouldBeUploadedToSharedCache(true);
                localResources.put(fileName, resource);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        Apps.addToEnvironment(appEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*",
                File.pathSeparator);
        Apps.addToEnvironment(appEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "libs/*",
                File.pathSeparator);
    }
}
