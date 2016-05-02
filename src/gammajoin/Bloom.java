/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import java.io.*;
import gammaSupport.BMap;
import gammaSupport.ReportError;

/**
 *
 * @author Vishal
 */
public class Bloom extends Thread {
    BufferedReader in; 
    PrintStream tupleOut;
    PrintStream mapOut;
    
    BMap bitMap;
    
    public Bloom(BufferedReader in, PrintStream tupleOut, PrintStream mapOut){
        this.tupleOut = tupleOut;
        this.mapOut = mapOut; 
        
        bitMap = BMap.makeBMap();
        
    }
    
    public void run(){
        String joinKey;
        try{
            String input; 
            while(true){
                input = in.readLine();
                if(input == null) break; 
                
                //Bloom filter calculation 
                joinKey = getJoinKey(input);
                bitMap.setValue(joinKey, true);
                
                tupleOut.println(input);
                tupleOut.flush();
            }
            tupleOut.flush();
            tupleOut.close();
            
            mapOut.println(bitMap.getBloomFilter());
        }
        
        catch (IOException e){
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }
        
    }
    
    //TODO: Actually get the join key 
    public String getJoinKey(String input){
        return input; 
    }
    
//    public int rowHash(String jkey){
//        return (jkey.hashCode() % BMap.splitLen);
//    }
//    
//    public int colHash(String jkey){
//        return (jkey.hashCode() % BMap.mapSize);
//    }
}
