package gammajoin;

import gammaSupport.*;
import basicConnector.*;
import java.io.*;

public class HSplit extends Thread{
    WriteEnd out[];
    ReadEnd in; 
    int jkey; 
    
    public HSplit(int jkey, ReadEnd recordStream, WriteEnd out1,
            WriteEnd out2, WriteEnd out3, WriteEnd out4) {
        this.in = recordStream;
        this.out = new WriteEnd[] {out1,out2,out3,out4};
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
