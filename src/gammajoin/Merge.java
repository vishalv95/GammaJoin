package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;

public class Merge extends Thread{
    ReadEnd in[];
    WriteEnd out; 
    String input[] = new String[4];
    static final String LAST = "|";
    
    
    public Merge(Connector in1, Connector in2, Connector in3, Connector in4, Connector out){
        this.in = new ReadEnd[] {in1.getReadEnd(), in2.getReadEnd(), in3.getReadEnd(), in4.getReadEnd()};
        this.out = out.getWriteEnd();
        ThreadList.add(this);        
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
        if(LAST.equals(input[line]))
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
