package gammajoin;

import basicConnector.*;
import gammaSupport.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

 public class SplitMIT {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/SplitMTest.txt");  // redirects standard out to file "out.txt"
        try {
            splitMTest();
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/SplitMTest.txt", "correctOutput/SplitMTest.txt", false); // test passes if files are equal
    }
    
    public void splitMTest() throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        BloomSimulator bs = new BloomSimulator(i);
        
        Connector o1 = new Connector("output1");
        Connector o2 = new Connector("output2");
        Connector o3 = new Connector("output3");
        Connector o4 = new Connector("output4");
        
        SplitM sm = new SplitM(i,o1,o2,o3,o4);
//        PrintMap p1 = new PrintMap(o1);
//        PrintMap p2 = new PrintMap(o2);
//        PrintMap p3 = new PrintMap(o3);
        PrintMap p4 = new PrintMap(o4);
        
        ThreadList.run(p4);
    }
    
}
