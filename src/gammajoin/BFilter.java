/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import gammaSupport.BMap;
import gammaSupport.ReportError;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author Vishal
 */
public class BFilter extends Thread {
    BufferedReader mapIn;
    BufferedReader tupleIn;
    PrintStream tupleOut;
    
    
    public BFilter(BufferedReader mapIn, BufferedReader tupleIn, PrintStream tupleOut){
        this.mapIn = mapIn;
        this.tupleIn = tupleIn;
        this.tupleOut = tupleOut;
        
    }
    
    public void run(){
        try {
            String input; 
            String joinKey;
            BMap map = BMap.makeBMap(tupleIn.readLine());
            while(true){
                input = tupleIn.readLine();
                if (input == null) break; 
                joinKey = getJoinKey(input);
                
                if(map.getValue(input)){
                    tupleOut.println(input);
                    tupleOut.flush();
                }
                
            } 
            tupleOut.flush();
            tupleOut.close();
        }
        
        catch (IOException e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }
    }
    
    
    public String getJoinKey(String input){
        return input; 
    }
    
    
}
