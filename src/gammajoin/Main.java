package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) {
        try {
//            nothingTest();
            join("parts", "odetails", 0, 1);
            
        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
        
        
    }
    
     public static void join(String r1name, String r2name, int jk1, int jk2) throws Exception {
            System.out.println( "Joining " + r1name + " with " + r2name );

            ThreadList.init();
            Connector c1 = new Connector("input1");
            ReadRelation r1 = new ReadRelation(r1name, c1); 
            Connector c2 = new Connector("input2");
            ReadRelation r2 = new ReadRelation(r2name, c2);
            Connector o = new Connector("output");
            HJoin hj = new HJoin(c1, c2, jk1, jk2, o);
            Print p = new Print(o);
            ThreadList.run(p);
    }
     
    public static void nothingTest() throws Exception{
        ThreadList.init();
        Connector c = new Connector("input1");
        Connector o = new Connector("output");
        DoNothing d = new DoNothing(c.getReadEnd(), o.getWriteEnd());
        ThreadList.run(d);
    }
     
}
