/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import java.io.*;
import gammaSupport.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Vishal
 */
public class MergeM extends Thread{
    BufferedReader[] mapIn;
    PrintStream mapOut;
    
    public MergeM(BufferedReader mapIn0, BufferedReader mapIn1, BufferedReader mapIn2, 
            BufferedReader mapIn3, PrintStream mapOut){
        this.mapIn = new BufferedReader[] {mapIn0, mapIn1, mapIn2, mapIn3};
        this.mapOut = mapOut;
    }
    
    public void run(){
        String input; 
        BMap mergeMap = BMap.makeBMap();
        try {
            for(int counter = 0; counter < BMap.splitLen; counter++){
                input = mapIn[counter].readLine();
                if(input == null) break; 
                mergeMap = BMap.or(mergeMap, BMap.makeBMap(input));
            }
            mapOut.println(mergeMap.getBloomFilter());
        }        
        catch (IOException e) {
                ReportError.msg(this.getClass().getName() + e);
        }
    }
    
}
    