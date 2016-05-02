/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import gammaSupport.ReportError;
import java.io.*;


/**
 *
 * @author Vishal
 */
public class HSplit extends Thread{
    static final int STREAM_NUM = 4;
    
    PrintStream out[];
    BufferedReader in; 
    
    public HSplit(BufferedReader recordStream, PrintStream out0,
            PrintStream out1, PrintStream out2, PrintStream out3 ) {
        this.in = recordStream;
        this.out = new PrintStream[] {out0,out1,out2,out3};
    }
    
    public void run(){
        String input;
        try {
            while(true){
                    input = in.readLine();
                    if(input == null) break;
                    out[hash(input)].println(input);
                    out[hash(input)].flush();
            }

            for(PrintStream outStream : out) {
                outStream.flush();
                outStream.close();
            }
        }
        
        catch (IOException e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }  
        
    }
    
    public int hash(String jkey){
        return (Math.abs(jkey.hashCode()) % STREAM_NUM);
    }
    
}
