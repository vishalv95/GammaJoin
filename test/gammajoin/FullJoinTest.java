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

public class FullJoinTest {
//  TODO: null value row?
    
    @Test
    public void testRun() {
        RegTest.Utility.redirectStdOut("testOutput/FullJoinTest.txt");  // redirects standard out to file "out.txt"
        try {
            fullJoinTest("parts", "odetails", 0, 1);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/FullJoinTest.txt", "correctOutput/FullJoinTest.txt", false); // test passes if files are equal
    }
    
    public void fullJoinTest(String r1name, String r2name, int jk1, int jk2) throws Exception {  
            ThreadList.init();
            
            Connector c1 = new Connector("input1");
            ReadRelation r1 = new ReadRelation(r1name, c1); 
            Connector c2 = new Connector("input2");
            ReadRelation r2 = new ReadRelation(r2name, c2);
            
            Connector[] splitConnectorA = new Connector[BMap.splitLen];
            Connector[] splitConnectorB = new Connector[BMap.splitLen];
            Connector[] bloomConnectorA = new Connector[BMap.splitLen];
            Connector[] mapConnectorA = new Connector[BMap.splitLen];
            Connector[] filterConnectorB = new Connector[BMap.splitLen];
            Connector[] joinConnector = new Connector[BMap.splitLen];

            Connector o = new Connector("finalOutput");

            for (int count = 0; count < BMap.splitLen; count++) {
                splitConnectorA[count] = new Connector("Asplit" + count);
                splitConnectorB[count] = new Connector("Bsplit" + count);
                bloomConnectorA[count] = new Connector("Abloom" + count);
                mapConnectorA[count] = new Connector("Amap" + count);
                filterConnectorB[count] = new Connector("Bfilter" + count);
                joinConnector[count] = new Connector("join" + count);
            }

            HSplit hsA = new HSplit(c1, splitConnectorA[0],
                    splitConnectorA[1], splitConnectorA[2], splitConnectorA[3], jk1);

            HSplit hsB = new HSplit(c2, splitConnectorB[0],
                    splitConnectorB[1], splitConnectorB[2], splitConnectorB[3], jk2);

            Bloom[] bloomBoxes = new Bloom[BMap.splitLen];
            BFilter[] bloomFilters = new BFilter[BMap.splitLen];
            HJoin[] joins = new HJoin[BMap.splitLen];

            for (int count = 0; count < BMap.splitLen; count++) {
                bloomBoxes[count] = new Bloom(splitConnectorA[count], bloomConnectorA[count], mapConnectorA[count], jk1);
                bloomFilters[count] = new BFilter(mapConnectorA[count], splitConnectorB[count], filterConnectorB[count], jk2);
                joins[count] = new HJoin(bloomConnectorA[count], filterConnectorB[count], jk1,jk2, joinConnector[count]);
            }

            Merge merge = new Merge(joinConnector[0],joinConnector[1],joinConnector[2],joinConnector[3],o);
            Print p = new Print(o);
            ThreadList.run(p);
    }
}
