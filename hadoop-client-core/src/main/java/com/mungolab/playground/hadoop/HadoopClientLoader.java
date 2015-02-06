package com.mungolab.playground.hadoop;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Vanja Komadinovic ( vanjakom@gmail.com )
 */
public class HadoopClientLoader {
    public enum HadoopClientVersion {
        CDH3u5,
        CDH5
    }

    public static HadoopClient getClient(HadoopClientVersion version) {
        switch (version) {
            case CDH3u5:
                return getHadoopClientCdh3u5();
            case CDH5:
                return getHadoopClientCdh5();
            default:
                System.out.println("Version unknown");
                return null;
        }
    }

    public static HadoopClient getHadoopClientCdh3u5() {
        ClassLoader classLoader = null;
        List<URL> urls = new LinkedList<URL>();
        try {
            urls.add(new URL(new URL("file:"), "./hadoop-client-cdh3/target/hadoop-client-cdh3-1.0-SNAPSHOT.jar"));

            for (String path: (new File("./hadoop-client-cdh3/lib/")).list()) {
                urls.add(new URL(new URL("file:"), "./hadoop-client-cdh3/lib/" + path));
            }

            classLoader = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]));
            //Thread.currentThread().setContextClassLoader(classLoader);
            return (HadoopClient)classLoader.loadClass("com.mungolab.playground.hadoop.HadoopClientImpl").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static HadoopClient getHadoopClientCdh5() {
        ClassLoader classLoader = null;
        List<URL> urls = new LinkedList<URL>();
        try {
            urls.add(new URL(new URL("file:"), "./hadoop-client-cdh5/target/hadoop-client-cdh5-1.0-SNAPSHOT.jar"));

            for (String path: (new File("./hadoop-client-cdh5/lib/")).list()) {
                urls.add(new URL(new URL("file:"), "./hadoop-client-cdh5/lib/" + path));
            }

            classLoader = URLClassLoader.newInstance(urls.toArray(new URL[urls.size()]));
            //Thread.currentThread().setContextClassLoader(classLoader);
            return (HadoopClient)classLoader.loadClass("com.mungolab.playground.hadoop.HadoopClientImpl").newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private static URL[] createUrlsFromTargetAndLib(String moduleRootPath) {
        // todo
        return null;
    }
}
