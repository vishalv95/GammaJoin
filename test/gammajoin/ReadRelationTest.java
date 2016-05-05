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

public class ReadRelationTest {
    
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/ReadRelationTest.txt");  // redirects standard out to file "out.txt"
        try {
            readRelationTest("parts");
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }
        RegTest.Utility.validate("testOutput/ReadRelationTest.txt", "correctOutput/ReadRelationTest.txt", false); // test passes if files are equal
    }
    
    public void readRelationTest(String rname) throws Exception {
        ThreadList.init();
        Connector o = new Connector("output");
        ReadRelation r = new ReadRelation(rname, o);
        Print p = new Print(o);
        ThreadList.run(p);
    }
    
}
