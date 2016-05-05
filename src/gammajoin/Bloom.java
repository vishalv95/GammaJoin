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
    
    public Bloom(Connector in, Connector tupleOut, Connector mapOut, int jkey){
        tupleOut.setRelation(in.getRelation());
        mapOut.setRelation(in.getRelation());
        
        this.in = in.getReadEnd(); 
        this.tupleOut = tupleOut.getWriteEnd();
        this.mapOut = mapOut.getWriteEnd(); 
        this.jkey = jkey;
        this.bitMap = BMap.makeBMap();
        
        ThreadList.add(this);
        
    }
    
    public void run(){
        Tuple input; 
        try{
            
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
}
