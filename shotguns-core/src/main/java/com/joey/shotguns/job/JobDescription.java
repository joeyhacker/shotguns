package com.joey.shotguns.job;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobDescription {

    private String appId;

    private String appName;

    private String appJar;

    private String mainClass;

    private int containers;

    private List<String> dependencies = new ArrayList();

    private Map<String, String> environment = new HashMap();

    private JobRequirement requirement;

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public JobRequirement getRequirement() {
        return requirement;
    }

    public void setRequirement(JobRequirement requirement) {
        this.requirement = requirement;
    }

    public String getMainClass() {
        return mainClass;
    }

    public void setMainClass(String mainClass) {
        this.mainClass = mainClass;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    public void setDependencies(List<String> dependencies) {
        this.dependencies = dependencies;
    }

    public Map<String, String> getEnvironment() {
        return environment;
    }

    public void setEnvironment(Map<String, String> environment) {
        this.environment = environment;
    }

    public String getAppJar() {
        return appJar;
    }

    public void setAppJar(String appJar) {
        this.appJar = appJar;
    }

    public int getContainers() {
        return containers;
    }

    public void setContainers(int containers) {
        this.containers = containers;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    @Override
    public String toString() {
        return "JobDescription{" +
                "appName='" + appName + '\'' +
                ", mainClass='" + mainClass + '\'' +
                ", appJar='" + appJar + '\'' +
                ", containers=" + containers +
                ", dependencies=" + dependencies +
                ", environment=" + environment +
                ", requirement=" + requirement +
                '}';
    }
}
