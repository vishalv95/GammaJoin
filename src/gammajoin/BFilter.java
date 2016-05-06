package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;


public class BFilter extends Thread {
    ReadEnd mapIn;
    ReadEnd tupleIn;
    WriteEnd tupleOut;
    int jkey;
    
    public BFilter(Connector mapIn, Connector tupleIn, Connector tupleOut, int jkey){
        tupleOut.setRelation(tupleIn.getRelation());
        
        this.mapIn = mapIn.getReadEnd();
        this.tupleIn = tupleIn.getReadEnd();
        this.tupleOut = tupleOut.getWriteEnd();
        this. jkey = jkey;
        
        ThreadList.add(this);
    }
    
    public void run(){
        try {
            Tuple input; 
            String joinKeyValue;
            BMap map = BMap.makeBMap(mapIn.getNextString());
            
            while(true){
                input = tupleIn.getNextTuple();
                if (input == null) break; 
                joinKeyValue = input.get(jkey);
                if(map.getValue(joinKeyValue))
                    tupleOut.putNextTuple(input);
            } 
            tupleOut.close();
        }
        
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }
    }
}
