package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;
import java.util.ArrayList;

public class HJoin extends Thread{
    ReadEnd inTuple1;
    ReadEnd inTuple2;
    WriteEnd outTuple;
    int jk1;
    int jk2;
    
    HJoin(Connector c1, Connector c2, int jk1, int jk2, Connector o) {
        this.inTuple1 = c1.getReadEnd();
        this.inTuple2 = c2.getReadEnd();
        this.outTuple = o.getWriteEnd();
        
        this.jk1 = jk1;
        this.jk2 = jk2; 
        
        ThreadList.add(this);
    }
    
    public void run(){
        ArrayList<Tuple> relATuples = new ArrayList<Tuple>();
        ArrayList<Tuple> relBTuples = new ArrayList<Tuple>();
        Tuple input; 
        Tuple output;
        try{
            while(true){
                input = inTuple1.getNextTuple();
                if(input == null) break;
                relATuples.add(input);
            }
            while(true){
                input = inTuple2.getNextTuple();
                if(input == null) break;
                relBTuples.add(input);
            }
        }
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }  
        finally{
            for(Tuple aTuple: relATuples){
                for(Tuple bTuple: relBTuples){
//                  Shouldn't this throw Array out of bounds if the tuples aren't joinable? 
                    try {                    
                        output = Tuple.join(aTuple, bTuple, jk1, jk2);
                        outTuple.putNextTuple(output);
                    } catch (Exception e) {
                        continue;
                    }
                }
            }
        }
        
        
    }
}
