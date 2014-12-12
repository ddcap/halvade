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

package be.ugent.intec.halvade.uploader;

/**
 *
 * @author ddecap
 */
public class Logger {
    /*
     * levels of debugging
     * 0: INFO
     * 1: DEBUG
     * 2: EXCEPTION
     * 3:ALL
     */
    private static int LEVEL = 2;
    private static final int EXCEPTION = 2;
    private static final int DEBUG = 1;
    private static final int INFO = 0;
    
    
    // TODO use configuration set LEVEL
    public static void SETLEVEL(int NEWLEVEL) {
        LEVEL = NEWLEVEL;
    }
    
    public static void EXCEPTION(Exception ex){
        if (LEVEL >= EXCEPTION) {
            System.err.println("[EXCEPTION] " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }
    
    public static void THROWABLE(Throwable ex){
        if (LEVEL >= EXCEPTION) {
            System.err.println("[EXCEPTION] " + ex.getLocalizedMessage());
            ex.printStackTrace();
        }
    }
    
    public static void DEBUG(String message) {
        if(LEVEL >= DEBUG)
            System.err.println("[" + Timer.getGlobalTime() + " - DEBUG] " + message);
    }
    
    public static void INFO(String message) {
        if(LEVEL >= INFO)
            System.err.println("[INFO] " + message);
    }
    
    public static void DEBUG(String message, int level) {
        if(LEVEL >= level)
            System.err.println("[" + Timer.getGlobalTime() + " - DEBUG] " + message);
    }
}
