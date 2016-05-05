package gammajoin;

import gammaSupport.*;
import basicConnector.*;
import java.io.*;

public class HSplit extends Thread{
    WriteEnd out[];
    ReadEnd in; 
    int jkey; 
    
    public HSplit(Connector in, Connector out1,
            Connector out2, Connector out3, Connector out4, int jkey) {
        this.in = in.getReadEnd();
        
        Relation rel = in.getRelation();
        out1.setRelation(rel);
        out2.setRelation(rel);
        out3.setRelation(rel);
        out4.setRelation(rel);
        
        this.out = new WriteEnd[] {out1.getWriteEnd(),out2.getWriteEnd(),
            out3.getWriteEnd(),out4.getWriteEnd()};
        
        this.jkey = jkey;
        ThreadList.add(this);
    }
    
    public void run(){
        Tuple input;
        try {
            while(true){
                    input = in.getNextTuple();
                    if(input == null) break;
                    out[BMap.myhash(input.get(jkey))].putNextTuple(input);
            }
            for(WriteEnd outStream : out) outStream.close();
        }
        
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }  
        
    }
}
