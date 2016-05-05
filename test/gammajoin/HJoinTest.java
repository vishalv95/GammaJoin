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

public class HJoinTest {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/HJoinTest.txt");  // redirects standard out to file "out.txt"
        try {
            hJoinTest("parts", "odetails", 0, 1);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/HJoinTest.txt", "correctOutput/HJoinTest.txt", false); // test passes if files are equal
    }
    
    public void hJoinTest(String r1name, String r2name, int jk1, int jk2) throws Exception {
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
