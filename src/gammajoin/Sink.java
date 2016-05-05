package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;


public class Sink extends Thread {
    ReadEnd in;
    public Sink(Connector in){
        this.in = in.getReadEnd();
        ThreadList.add(this);
    }
    
    public void run(){
        Tuple input;
        try{
            while(true){
                input = in.getNextTuple();
                if(input == null) break;
            }
        }
        
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }
}
