package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

public class MergeM extends Thread{
    ReadEnd[] mapIn;
    WriteEnd mapOut;
    
    public MergeM(Connector mapIn0, Connector mapIn1, Connector mapIn2, Connector mapIn3, Connector mapOut){
        mapOut.setRelation(mapIn0.getRelation());
        this.mapIn = new ReadEnd[] {mapIn0.getReadEnd(), mapIn1.getReadEnd(), 
            mapIn2.getReadEnd(), mapIn3.getReadEnd()};
        this.mapOut = mapOut.getWriteEnd();
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
    