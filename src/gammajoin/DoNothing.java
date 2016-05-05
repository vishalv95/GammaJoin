package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;


public class DoNothing extends Thread{
    ReadEnd in; 
    WriteEnd out;
    
    public DoNothing(Connector in, Connector out){
        out.setRelation(in.getRelation());
        this.in = in.getReadEnd();
        this.out = out.getWriteEnd();
        
        ThreadList.add(this);
    }
    
    public void run(){
        Tuple input;
        try{
            while(true){
                input = in.getNextTuple();
                if(input == null) break;
                out.putNextTuple(input);
            }
            out.close();
        }
        
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }
}
