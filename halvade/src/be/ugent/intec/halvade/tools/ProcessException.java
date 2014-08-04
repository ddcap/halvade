/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package be.ugent.intec.halvade.tools;

/**
 *
 * @author ddecap
 */
public class ProcessException extends InterruptedException {
    private String programName;
    private int errorCode;

    public ProcessException(String name, int error) {
        super();
        programName = name;
        errorCode = error;
    }

    @Override
    public String toString() {
        return programName + " exited with code " + errorCode; //To change body of generated methods, choose Tools | Templates.
    }
    
    
    
}
