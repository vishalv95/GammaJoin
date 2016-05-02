package gammajoin;

import java.io.*;
import gammaSupport.*;

public class DoNothing extends Thread{
    BufferedReader in; 
    PrintStream out;
    public DoNothing(BufferedReader in, PrintStream out){
        this.in = in;
        this.out = out;
    }
    
    public void run(){
        String input;
        try{
            while(true){
                input = in.readLine();
                if(input == null) break;
                out.println(input);
                out.flush();
            }
            out.flush();
            out.close();
        }
        
        catch (IOException e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }
}
