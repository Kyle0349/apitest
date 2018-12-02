package com.kyle.utils;

import java.util.Properties;

public class ConfigUtils {


    public static Properties p =new Properties();

    static {
        try{
            p.load(ClassLoader.getSystemResourceAsStream("phoenix.properties"));
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static String getProp(String PropKey){
        return p.getProperty(PropKey);
    }


}
