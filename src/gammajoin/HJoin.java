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
    static boolean initialized = false; 
    
    HJoin(Connector c1, Connector c2, int jk1, int jk2, Connector o) {
        this.inTuple1 = c1.getReadEnd();
        this.inTuple2 = c2.getReadEnd();
        this.outTuple = o.getWriteEnd();
        
        this.jk1 = jk1;
        this.jk2 = jk2; 
        
        if(initialized) ThreadList.add(this);
        
        if(!initialized){
            initialized = true; 
            
            Relation joinRelation = Relation.join(inTuple1.getRelation(), inTuple2.getRelation(), jk1,jk2);
            
            Connector[] splitConnectorA = new Connector[BMap.splitLen];
            Connector[] splitConnectorB = new Connector[BMap.splitLen];
            Connector[] bloomConnectorA = new Connector[BMap.splitLen];
            Connector[] mapConnectorA = new Connector[BMap.splitLen];
            Connector[] filterConnectorB = new Connector[BMap.splitLen];
            Connector[] joinConnector = new Connector[BMap.splitLen];

            for (int count = 0; count < BMap.splitLen; count++) {
                System.out.println(count);
                splitConnectorA[count] = new Connector("Asplit" + count);
                splitConnectorA[count].setRelation(inTuple1.getRelation());

                splitConnectorB[count] = new Connector("Bsplit" + count);
                splitConnectorB[count].setRelation(inTuple2.getRelation());

                bloomConnectorA[count] = new Connector("Abloom" + count);
                bloomConnectorA[count].setRelation(inTuple1.getRelation());

                mapConnectorA[count] = new Connector("Amap" + count);
                mapConnectorA[count].setRelation(inTuple1.getRelation());

                filterConnectorB[count] = new Connector("Bfilter" + count);
                filterConnectorB[count].setRelation(inTuple2.getRelation());

                joinConnector[count] = new Connector("join" + count);
                joinConnector[count].setRelation(joinRelation);
            }
            
            o.setRelation(joinRelation);

            HSplit hsA = new HSplit(jk1, inTuple1, splitConnectorA[0].getWriteEnd(),
                    splitConnectorA[1].getWriteEnd(), splitConnectorA[2].getWriteEnd(),
                    splitConnectorA[3].getWriteEnd());
            HSplit hsB = new HSplit(jk2, inTuple2, splitConnectorB[0].getWriteEnd(),
                    splitConnectorB[1].getWriteEnd(), splitConnectorB[2].getWriteEnd(),
                    splitConnectorB[3].getWriteEnd());
            Bloom[] bloomBoxes = new Bloom[BMap.splitLen];
            BFilter[] bloomFilters = new BFilter[BMap.splitLen];
            HJoin[] joins = new HJoin[BMap.splitLen];



            for (int count = 0; count < BMap.splitLen; count++) {
                bloomBoxes[count] = new Bloom(jk1, splitConnectorA[count].getReadEnd(), bloomConnectorA[count].getWriteEnd(), mapConnectorA[count].getWriteEnd());
                bloomFilters[count] = new BFilter(jk2, splitConnectorB[count].getReadEnd(), mapConnectorA[count].getReadEnd(), filterConnectorB[count].getWriteEnd());
                joins[count] = new HJoin(bloomConnectorA[count], filterConnectorB[count], jk1,jk2, joinConnector[count]);
            }
            Merge merge = new Merge(joinConnector[0].getReadEnd(),joinConnector[1].getReadEnd(),joinConnector[2].getReadEnd(),joinConnector[3].getReadEnd(),o.getWriteEnd());
            
        }
        
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
