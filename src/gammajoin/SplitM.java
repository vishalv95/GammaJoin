/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;
import gammaSupport.*;
import java.io.*;

/**
 *
 * @author Vishal
 */
public class SplitM extends Thread{
    BufferedReader mapIn;
    PrintStream[] mapOut; 
    
    public SplitM(BufferedReader mapIn, PrintStream mapOut0, PrintStream mapOut1, 
            PrintStream mapOut2, PrintStream mapOut3){
        this.mapIn = mapIn;
        this.mapOut = new PrintStream[] {mapOut0, mapOut1, mapOut2, mapOut3};
        
    }
    
    public void run(){
        String input;
        String output;

        try{
            input = mapIn.readLine();
            //Copy each row of the input and zero out all other rows 
            for (int counter = 0; counter < BMap.splitLen; counter++){
                output = BMap.mask(input, counter);
                mapOut[counter].println(output);
                mapOut[counter].flush();
            }
            
            //flush and close write ends 
            for (PrintStream ps: mapOut){
                ps.flush();
                ps.close();
            }
            
        }
        catch (IOException e) {
                ReportError.msg(this.getClass().getName() + e);
        }
    }
}
