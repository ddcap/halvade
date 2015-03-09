/*
 * Copyright (C) 2014 ddecap
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
                stream.println(prefix + "[" + Timer.getGlobalTime() + "] " + line);
        }
        catch (IOException ioe) {
            Logger.EXCEPTION(ioe);
        }
    }
}
