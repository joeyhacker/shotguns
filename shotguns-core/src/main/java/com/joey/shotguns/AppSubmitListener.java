package com.joey.shotguns;

public abstract class AppSubmitListener {

    public abstract void onProgress(String status, float progress);

    public abstract void onFinish(String status, long finishTime);

    public abstract void onFailure(String status, String diagnostics);
}
