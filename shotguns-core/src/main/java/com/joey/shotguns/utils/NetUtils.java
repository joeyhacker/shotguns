package com.joey.shotguns.utils;

import org.apache.commons.lang.StringUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class NetUtils {

    public static String getHostName() {
        String ret = "";
        try {
            InetAddress addr = InetAddress.getLocalHost();
            ret = addr.getHostName().toString();
            if (StringUtils.isBlank(ret)) {
                ret = addr.getHostAddress().toString();
            }
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return ret;
    }


    public static void main(String[] args) {

        System.out.println(getHostName());
    }
}
