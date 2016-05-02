package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

public class MergeM extends Thread{
    ReadEnd[] mapIn;
    WriteEnd mapOut;
    
    public MergeM(ReadEnd mapIn0, ReadEnd mapIn1, ReadEnd mapIn2, 
            ReadEnd mapIn3, WriteEnd mapOut){
        this.mapIn = new ReadEnd[] {mapIn0, mapIn1, mapIn2, mapIn3};
        this.mapOut = mapOut;
        ThreadList.add(this);
    }
    
    public void run(){
        String input; 
        BMap mergeMap = BMap.makeBMap();
        try {
            for(int counter = 0; counter < BMap.splitLen; counter++){
                input = mapIn[counter].getNextString();
                if(input == null) break; 
                mergeMap = BMap.or(mergeMap, BMap.makeBMap(input));
            }
            mapOut.putNextString(mergeMap.getBloomFilter());
            mapOut.close();
        }        
        catch (Exception e) {
                ReportError.msg(this.getClass().getName() + e);
        }
    }
    
}
    