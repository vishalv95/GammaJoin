package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

public class SplitM extends Thread{
    ReadEnd mapIn;
    WriteEnd[] mapOut; 
    
    public SplitM(Connector mapIn, Connector mapOut1, Connector mapOut2, 
            Connector mapOut3, Connector mapOut4){
        Relation rel = mapIn.getRelation();
        mapOut1.setRelation(rel);
        mapOut2.setRelation(rel);
        mapOut3.setRelation(rel);
        mapOut4.setRelation(rel);
        
        this.mapIn = mapIn.getReadEnd();
        this.mapOut = new WriteEnd[] {mapOut1.getWriteEnd(), mapOut2.getWriteEnd(),
            mapOut3.getWriteEnd(), mapOut4.getWriteEnd()};
        ThreadList.add(this);

    }
    
    public void run(){
        String input;
        String output;

        try{
            input = mapIn.getNextString();
            
            //Copy each row of the input and zero out all other rows 
            for (int counter = 0; counter < BMap.splitLen; counter++){
                output = BMap.mask(input, counter);
                mapOut[counter].putNextString(output);
            }
            
            //Close write ends 
            for (WriteEnd ps: mapOut) ps.close();
        }
        catch (Exception e) {
                ReportError.msg(this.getClass().getName() + e);
        }
    }
}
