package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;

public class HJoin extends Thread{
    ReadEnd inTuple1;
    ReadEnd inTuple2;
    WriteEnd outTuple;
    int jk1;
    int jk2;
    
    public HJoin(Connector c1, Connector c2, int jk1, int jk2, Connector o) {
        o.setRelation(Relation.join(c1.getRelation(), c2.getRelation(), jk1, jk2));
        this.inTuple1 = c1.getReadEnd();
        this.inTuple2 = c2.getReadEnd();
        this.outTuple = o.getWriteEnd();
        
        this.jk1 = jk1;
        this.jk2 = jk2; 
        
        ThreadList.add(this);
            
        
    }
    
    public void run(){
        HashMap<String, LinkedList<Tuple>> relATuples = new HashMap<String, LinkedList<Tuple>>();
        
        Tuple input;
        Tuple bTuple;
        Tuple output;
        LinkedList ll;
        try{
            while(true){
                input = inTuple1.getNextTuple();
                if(input == null) break;
                if(relATuples.get(input.get(jk1)) == null)
                    ll = new LinkedList<Tuple>();
                else
                    ll = relATuples.get(input.get(jk1));
                
                ll.add(input);
                relATuples.put(input.get(jk1), ll);
                
            }
            
            while(true){
                bTuple = inTuple2.getNextTuple();
                if(bTuple == null) break;
                if(relATuples.get(bTuple.get(jk2)) == null) continue;
                else {
                    for(Tuple aTuple: relATuples.get(bTuple.get(jk2))){
                        output = Tuple.join(aTuple, bTuple, jk1, jk2);
                        outTuple.putNextTuple(output);
                    }
                }
            }
            outTuple.close();
        }
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }  
    }
} 
