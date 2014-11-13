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
public class QualityException extends Exception {
    private boolean illegal = false;
    private byte errorCode;

    QualityException(byte error) {
        super();
        errorCode = error;
    }
    
    QualityException() {
        super();
        illegal = true;
    }

    @Override
    public String toString() {
        if(illegal) 
            return "Illegal Phred 64 or Phred 33 encoding";
        else 
            return "Error converting Phred 64 to Phred 33 quality : " + (int)errorCode; 
    }
}
