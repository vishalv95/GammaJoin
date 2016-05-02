package gammajoin;

import java.io.*;
import gammaSupport.*;
import basicConnector.*;

public class Bloom extends Thread {
    ReadEnd in; 
    WriteEnd tupleOut;
    WriteEnd mapOut;
    int jkey;
    
    BMap bitMap;
    
    public Bloom(int jkey, ReadEnd in, WriteEnd tupleOut, WriteEnd mapOut){
        this.tupleOut = tupleOut;
        this.mapOut = mapOut; 
        this.jkey = jkey;
        bitMap = BMap.makeBMap();
        ThreadList.add(this);
        
    }
    
    public void run(){
        String joinKey;
        try{
            Tuple input; 
            while(true){
                input = in.getNextTuple();
                if(input == null) break; 
                
                //Bloom filter calculation 
                bitMap.setValue(input.get(jkey), true);
                tupleOut.putNextTuple(input);   
            }
            tupleOut.close();
            
            mapOut.putNextString(bitMap.getBloomFilter());
            mapOut.close();
        }
        
        catch (Exception e){
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }
    }
    
//    public int rowHash(String jkey){
//        return (jkey.hashCode() % BMap.splitLen);
//    }
//    
//    public int colHash(String jkey){
//        return (jkey.hashCode() % BMap.mapSize);
//    }
}
