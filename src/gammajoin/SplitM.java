package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

public class SplitM extends Thread{
    ReadEnd mapIn;
    WriteEnd[] mapOut; 
    
    public SplitM(ReadEnd mapIn, WriteEnd mapOut1, WriteEnd mapOut2, 
            WriteEnd mapOut3, WriteEnd mapOut4){
        this.mapIn = mapIn;
        this.mapOut = new WriteEnd[] {mapOut1, mapOut2, mapOut3, mapOut4};
    }
    
    public void run(){
        String input;
        String output;

        try{
            input = mapIn.getNextString();
            
            //Copy each row of the input and zero out all other rows 
            for (int counter = 0; counter < BMap.splitLen; counter++){
                output = BMap.mask(input, counter);
                mapOut[counter].putNextString(output);
            }
            
            //Close write ends 
            for (WriteEnd ps: mapOut) ps.close();
        }
        catch (Exception e) {
                ReportError.msg(this.getClass().getName() + e);
        }
    }
}
