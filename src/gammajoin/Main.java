package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {
        try {
            join("parts", "odetails", 0, 1);
            
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
    }
    
     public static void join(String r1name, String r2name, int jk1, int jk2) throws Exception {
            System.out.println( "Joining " + r1name + " with " + r2name );

            ThreadList.init();
            Connector c1 = new Connector("input1");
            ReadRelation r1 = new ReadRelation(r1name, c1); 
            Connector c2 = new Connector("input2");
            ReadRelation r2 = new ReadRelation(r2name, c2);
            Connector o = new Connector("output");
            HJoin hj = new HJoin(c1, c2, jk1, jk2, o);
            Print p = new Print(o);
            ThreadList.run(p);
    }
     

//    
//                Relation joinRelation = Relation.join(inTuple1.getRelation(), inTuple2.getRelation(), jk1,jk2);
//            
//            Connector[] splitConnectorA = new Connector[BMap.splitLen];
//            Connector[] splitConnectorB = new Connector[BMap.splitLen];
//            Connector[] bloomConnectorA = new Connector[BMap.splitLen];
//            Connector[] mapConnectorA = new Connector[BMap.splitLen];
//            Connector[] filterConnectorB = new Connector[BMap.splitLen];
//            Connector[] joinConnector = new Connector[BMap.splitLen];
//
//            for (int count = 0; count < BMap.splitLen; count++) {
//                System.out.println(count);
//                splitConnectorA[count] = new Connector("Asplit" + count);
//                splitConnectorA[count].setRelation(inTuple1.getRelation());
//
//                splitConnectorB[count] = new Connector("Bsplit" + count);
//                splitConnectorB[count].setRelation(inTuple2.getRelation());
//
//                bloomConnectorA[count] = new Connector("Abloom" + count);
//                bloomConnectorA[count].setRelation(inTuple1.getRelation());
//
//                mapConnectorA[count] = new Connector("Amap" + count);
//                mapConnectorA[count].setRelation(inTuple1.getRelation());
//
//                filterConnectorB[count] = new Connector("Bfilter" + count);
//                filterConnectorB[count].setRelation(inTuple2.getRelation());
//
//                joinConnector[count] = new Connector("join" + count);
//                joinConnector[count].setRelation(joinRelation);
//            }
//            
//            o.setRelation(joinRelation);
//
//            HSplit hsA = new HSplit(jk1, inTuple1, splitConnectorA[0].getWriteEnd(),
//                    splitConnectorA[1].getWriteEnd(), splitConnectorA[2].getWriteEnd(),
//                    splitConnectorA[3].getWriteEnd());
//            HSplit hsB = new HSplit(jk2, inTuple2, splitConnectorB[0].getWriteEnd(),
//                    splitConnectorB[1].getWriteEnd(), splitConnectorB[2].getWriteEnd(),
//                    splitConnectorB[3].getWriteEnd());
//            Bloom[] bloomBoxes = new Bloom[BMap.splitLen];
//            BFilter[] bloomFilters = new BFilter[BMap.splitLen];
//            HJoin[] joins = new HJoin[BMap.splitLen];
//
//
//
//            for (int count = 0; count < BMap.splitLen; count++) {
//                bloomBoxes[count] = new Bloom(jk1, splitConnectorA[count].getReadEnd(), bloomConnectorA[count].getWriteEnd(), mapConnectorA[count].getWriteEnd());
//                bloomFilters[count] = new BFilter(jk2, splitConnectorB[count].getReadEnd(), mapConnectorA[count].getReadEnd(), filterConnectorB[count].getWriteEnd());
//                joins[count] = new HJoin(bloomConnectorA[count], filterConnectorB[count], jk1,jk2, joinConnector[count]);
//            }
//            Merge merge = new Merge(joinConnector[0].getReadEnd(),joinConnector[1].getReadEnd(),joinConnector[2].getReadEnd(),joinConnector[3].getReadEnd(),o.getWriteEnd());

    
}
