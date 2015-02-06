package com.mungolab.playground.hadoop;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Vanja Komadinovic ( vanjakom@gmail.com )
 */
public interface HadoopClient {
    public void initialize(String path);

    public OutputStream streamForWriting(String path);

    public InputStream streamForReading(String path);

    public String[] list(String path);
}
