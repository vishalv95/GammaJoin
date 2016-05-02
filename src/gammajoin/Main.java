/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import basicConnector.Connector;
import gammaSupport.ThreadList;

/**
 *
 * @author Vishal
 */
public class Main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
       
        
    }
    
     public void join(String r1name, String r2name, int jk1, int jk2) throws Exception {
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
    
}
