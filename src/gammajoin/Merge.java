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
public class Merge extends Thread{
    BufferedReader in[];
    PrintStream out; 
    String input[] = new String[4];
    static final String LAST = "|";
    static final int STREAM_COUNT = 4;
    
    public Merge(BufferedReader in0, BufferedReader in1, 
            BufferedReader in2, BufferedReader in3, PrintStream out){
        this.in = new BufferedReader[] {in0, in1, in2, in3};
        this.out = out;
        
    }
    
    public void run(){
        int nullCount = 0;
        while(true){
            for(int i = 0; i < 4; i++){
                readInput(i);
                if (input[i].equals(LAST) && nullCount == STREAM_COUNT) break ;
                else if(input[i].equals(LAST)) nullCount++;
                
                else{
                    nullCount = 0;
                    out.println(input[i]);
                    out.flush();
                }
            }
            if (nullCount == STREAM_COUNT) break;
        }
        out.flush();
        out.close();
    }
    
    
    void readInput(int line){
        if(input[line].equals(LAST))
            return;

        try {
            input[line] = in[line].readLine();
            if(input[line] == null){
                input[line] = LAST;
            }
            
        } 
        catch (IOException e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }        
    }
}
