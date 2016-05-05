package gammajoin;

import gammaSupport.*;
import basicConnector.*;
import java.io.*;

public class BloomSimulator extends Thread {
    WriteEnd mapOut;
    BMap map;
    String[] keys;
    public BloomSimulator(Connector o){
        o.setRelation(Relation.dummy);
        this.mapOut = o.getWriteEnd();
        this.map = BMap.makeBMap();
        this.keys = new String[] { "10506", "10507", "10508", "10509", "10601", "10701", "10900","10800"};
    }
    
    public void run(){
        try {
            for (String key:keys)map.setValue(key, true);
            mapOut.putNextString(map.getBloomFilter());
            mapOut.close();
        }
        catch (Exception e) {
            ReportError.msg("Print: " + this.getClass().getName() + e);
        }
    }
    
}
