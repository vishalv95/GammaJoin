package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

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
    }
    
    public void run(){
        Connector[] splitConnectorA = new Connector[4];
        Connector[] splitConnectorB = new Connector[4];
        Connector[] bloomConnectorA = new Connector[4];
        Connector[] mapConnectorA = new Connector[4];
        Connector[] filterConnectorB = new Connector[4];
        Connector[] joinConnector = new Connector[4];
        
        HSplit hsA = new HSplit(jk1, inTuple1, splitConnectorA[0].getWriteEnd(), 
                splitConnectorA[1].getWriteEnd(), splitConnectorA[2].getWriteEnd(), 
                splitConnectorA[3].getWriteEnd());
                            
        HSplit hsB = new HSplit(jk2, inTuple2, splitConnectorB[0].getWriteEnd(), 
                splitConnectorB[1].getWriteEnd(), splitConnectorB[2].getWriteEnd(), 
                splitConnectorB[3].getWriteEnd());
        
        Bloom[] bloomBoxes = new Bloom[4];
        BFilter[] bloomFilters = new BFilter[4];
        
        for (int count = 0; count < 4; count++){
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
                       
            bloomBoxes[count] = new Bloom(jk1, splitConnectorA[count].getReadEnd(), bloomConnectorA[count].getWriteEnd(), mapConnectorA[count].getWriteEnd());
            bloomFilters[count] = new BFilter(jk2, splitConnectorB[count].getReadEnd(), mapConnectorA[count].getReadEnd(), filterConnectorB[count].getWriteEnd());
            
        }
    }
    
}
