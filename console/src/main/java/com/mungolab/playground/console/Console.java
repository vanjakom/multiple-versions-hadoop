package com.mungolab.playground.console;

import com.mungolab.playground.hadoop.HadoopClient;
import com.mungolab.playground.hadoop.HadoopClientLoader;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Vanja Komadinovic ( vanjakom@gmail.com )
 */
public class Console {
    public static void main(String[] args) {
        HadoopClient clientCDH3 = HadoopClientLoader.getHadoopClientCdh3u5();
        HadoopClient clientCDH5 = HadoopClientLoader.getHadoopClientCdh5();

        clientCDH3.initialize("hdfs://clusterCDH3/user/hadoop/");
        clientCDH5.initialize("hdfs://clusterCDH5/user/hadoop/");

        String[] paths = clientCDH3.list("hdfs://clusterCDH3/user/hadoop/");
        for (String path: paths) {
            System.out.println(path);
        }

        paths = clientCDH5.list("hdfs://clusterCDH5/user/hadoop/");
        for (String path: paths) {
            System.out.println(path);
        }

        InputStream inputStream =
                clientCDH3.streamForReading("/user/hadoop/source");
        OutputStream outputStream =
                clientCDH5.streamForWriting("/user/hadoop/destination");
        try {
            IOUtils.copy(inputStream, outputStream);
            inputStream.close();
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("End");
    }
}
