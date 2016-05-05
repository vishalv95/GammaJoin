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

public class DoNothingTest {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/DoNothing.txt");  // redirects standard out to file "out.txt"
        try {
            doNothingTest("parts");
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/DoNothing.txt", "correctOutput/DoNothing.txt", false); // test passes if files are equal
    }
    
    public void doNothingTest(String rname) throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        ReadRelation r = new ReadRelation(rname, i);
        
        Connector o = new Connector("output");
        
        DoNothing d = new DoNothing(i,o);
        Print p = new Print(o);
        
        ThreadList.run(p);
    }
}
