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

public class HSplitTest {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/HSplitTest.txt");  // redirects standard out to file "out.txt"
        try {
            hSplitTest("parts", 0);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/HSplitTest.txt", "correctOutput/HSplitTest.txt", false); // test passes if files are equal
    }
    
    public void hSplitTest(String rname, int jk1) throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        ReadRelation r = new ReadRelation(rname, i);
        
        Connector o1 = new Connector("output1");
        Connector o2 = new Connector("output2");
        Connector o3 = new Connector("output3");
        Connector o4 = new Connector("output4");
        
        HSplit h = new HSplit(i,o1,o2,o3,o4,jk1);
//        Print p1 = new Print(o1);
//        Print p2 = new Print(o2);
//        Print p3 = new Print(o3);
        Print p4 = new Print(o4);
        
        ThreadList.run(p4);
    }
    
}
