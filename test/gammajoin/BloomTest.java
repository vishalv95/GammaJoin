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

public class BloomTest {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/BloomTest.txt");  // redirects standard out to file "out.txt"
        try {
            bloomTest("parts", 0);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/BloomTest.txt", "correctOutput/BloomTest.txt", false); // test passes if files are equal
    }
    
    public void bloomTest(String rname, int jk1) throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        ReadRelation r = new ReadRelation(rname, i);
        
        Connector o1 = new Connector("output1");
        Connector o2 = new Connector("output2");
        
        Bloom b = new Bloom(i,o1,o2,jk1);
        Print p1 = new Print(o1);
//        PrintMap p2 = new PrintMap(o2);
        
        ThreadList.run(p1);
    }
}
