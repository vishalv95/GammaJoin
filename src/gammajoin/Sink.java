package gammajoin;

import gammaSupport.ReportError;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintStream;

public class Sink extends Thread {
    BufferedReader in;
    public Sink(BufferedReader in){
        this.in = in;
    }
    
    public void run(){
        String input;
        try{
            while(true){
                input = in.readLine();
                if(input == null) break;
            }
        }
        
        catch (IOException e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }
}
