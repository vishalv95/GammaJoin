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

public class MergeMIT {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/MergeMTest.txt");  // redirects standard out to file "out.txt"
        try {
            mergeMTest();
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/MergeMTest.txt", "correctOutput/MergeMTest.txt", false); // test passes if files are equal
    }
    
    public void mergeMTest() throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        BloomSimulator bs = new BloomSimulator(i);
        
        Connector i1 = new Connector("input1");
        Connector i2 = new Connector("input2");
        Connector i3 = new Connector("input3");
        Connector i4 = new Connector("input4");
        
        SplitM sm = new SplitM(i, i1,i2,i3,i4);
        
        Connector o = new Connector("output");
        MergeM mm = new MergeM(i1,i2,i3,i4,o);
        
        PrintMap p = new PrintMap(o);
        
        ThreadList.run(p);
    }
    
}
