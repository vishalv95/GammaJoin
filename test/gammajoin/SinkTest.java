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

public class SinkTest {
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/Sink.txt");  // redirects standard out to file "out.txt"
        try {
            sinkTest("parts");
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/Sink.txt", "correctOutput/Sink.txt", false); // test passes if files are equal
    }
    
    public void sinkTest(String rname) throws Exception {
        ThreadList.init();
        Connector i = new Connector("input");
        ReadRelation r = new ReadRelation(rname, i);
        Sink d = new Sink(i);
        ThreadList.run(d);
    }
}
