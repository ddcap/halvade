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

package be.ugent.intec.halvade.tools;

import java.util.Arrays;

/**
 *
 * @author ddecap
 */
public class ProcessException extends InterruptedException {
    private String programName;
    private String[] command;
    private int errorCode;

    public ProcessException(String name, int error) {
        super();
        command = null;
        programName = name;
        errorCode = error;
    }    
    
    public ProcessException(String[] command, String name, int error) {
        super();
        this.command = command;
        programName = name;
        errorCode = error;
    }

    @Override
    public String toString() {
        if(command == null)
            return programName + " exited with code " + errorCode;
        else
            return "command: " + Arrays.toString(command) + "\n" + programName + " exited with code " + errorCode; 
    }
    
    
    
}
