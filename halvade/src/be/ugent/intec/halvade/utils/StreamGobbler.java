/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package be.ugent.intec.halvade.utils;

import java.io.*;

/**
 *
 * @author ddecap
 */
public class StreamGobbler extends Thread {
    InputStream is;
    PrintStream stream;
    String prefix;

    public StreamGobbler(InputStream is, PrintStream stream) {
        this.is = is;
        this.stream = stream;
        prefix = "";
    }
    
    public StreamGobbler(InputStream is, PrintStream stream, String prefix) {
        this.is = is;
        this.stream = stream;
        this.prefix = prefix;
    }

    @Override
    public void run() {
        try {
            InputStreamReader isr = new InputStreamReader(is);
            BufferedReader br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null)
                stream.println(prefix + line);
        }
        catch (IOException ioe) {
            Logger.EXCEPTION(ioe);
        }
    }
}
