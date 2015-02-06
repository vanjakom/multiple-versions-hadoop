package com.mungolab.playground.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

/**
 * @author Vanja Komadinovic ( vanjakom@gmail.com )
 */
public class HadoopClientImpl implements HadoopClient {
    private FileSystem fs = null;

    public HadoopClientImpl() {
        System.out.println("Loading HadoopClient CDH3");
    }

    @Override
    public void initialize(String path) {
        try {
            ClassLoader threadLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());
            Configuration config = new Configuration();
            config.set("fs.hdfs.impl", (new org.apache.hadoop.hdfs.DistributedFileSystem()).getClass().getName());
            this.fs = FileSystem.get(new URI(path), config);
            Thread.currentThread().setContextClassLoader(threadLoader);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public OutputStream streamForWriting(String path) {
        try {
            if (fs != null) {
                return fs.create(new Path(path));
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public InputStream streamForReading(String path) {
        try {
            if (fs != null) {
                return fs.open(new Path(path));
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String[] list(String path) {
        try {
            if (fs != null) {
                if (fs.exists(new Path(path))) {
                    FileStatus[] files = fs.listStatus(new Path(path));
                    if (files.length > 0) {
                        String[] paths = new String[files.length];
                        for (int i = 0; i < files.length; i++) {
                            paths[i] = files[i].getPath().toString();
                        }
                        return paths;
                    } else {
                        return new String[0];
                    }
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
