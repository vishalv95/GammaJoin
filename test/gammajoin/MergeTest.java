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

public class MergeTest {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/MergeTest.txt");  // redirects standard out to file "out.txt"
        try {
            mergeTest("parts", 0);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/MergeTest.txt", "correctOutput/MergeTest.txt", false); // test passes if files are equal
    }
    
    public void mergeTest(String rname, int jk1) throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        ReadRelation r = new ReadRelation(rname, i);
        
        Connector o1 = new Connector("output1");
        Connector o2 = new Connector("output2");
        Connector o3 = new Connector("output3");
        Connector o4 = new Connector("output4");
        
        Connector of = new Connector("outputFinal");
        
        HSplit h = new HSplit(i,o1,o2,o3,o4,jk1);
        Merge m = new Merge(o1,o2,o3,o4,of);
        Print p = new Print(of);
        
        ThreadList.run(p);
    }
    
}
