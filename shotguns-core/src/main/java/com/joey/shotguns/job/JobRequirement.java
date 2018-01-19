package com.joey.shotguns.job;

public class JobRequirement {

    private int parallelism;
    private int memory;
    private int virtualCores;

    public int getParallelism() {
        return parallelism;
    }

    public void setParallelism(int parallelism) {
        this.parallelism = parallelism;
    }

    public int getMemory() {
        return memory;
    }

    public void setMemory(int memory) {
        this.memory = memory;
    }

    public int getVirtualCores() {
        return virtualCores;
    }

    public void setVirtualCores(int virtualCores) {
        this.virtualCores = virtualCores;
    }


    @Override
    public String toString() {
        return "JobRequirement{" +
                "parallelism=" + parallelism +
                ", memory=" + memory +
                ", virtualCores=" + virtualCores +
                '}';
    }
}
