package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

public class Merge extends Thread{
    ReadEnd in[];
    WriteEnd out; 
    String input[] = new String[4];
    static final String LAST = "|";
    
    
    public Merge(ReadEnd in1, ReadEnd in2, 
            ReadEnd in3, ReadEnd in4, WriteEnd out){
        this.in = new ReadEnd[] {in1, in2, in3, in4};
        this.out = out;
        
    }
    
    public void run(){
        int nullCount = 0;
        try{
            while(true){
                for(int i = 0; i < 4; i++){
                    readInput(i);
                    if (input[i].equals(LAST) && nullCount == BMap.splitLen) break ;
                    else if(input[i].equals(LAST)) nullCount++;

                    else{
                        nullCount = 0;
                        out.putNextTuple(Tuple.makeTupleFromPipeData(input[i]));

                    }
                }
                if (nullCount == BMap.splitLen) break;
            }
            out.close();
        }
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }  
    }
    
    
    void readInput(int line){
        Tuple trial;
        if(input[line].equals(LAST))
            return;

        try {
            trial = in[line].getNextTuple();
            if(trial == null) input[line] = LAST;
            else input[line] = trial.toString();            
        } 
        catch (Exception e) {
            ReportError.msg(this.getClass().getName() + " WriteReversedThread run: " + e);
        }        
    }
}
