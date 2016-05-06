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
            fullJoinTest("client", "viewing", 0, 0);
            fullJoinTest("orders", "odetails", 0, 0);
        } catch (Exception ex) {
            Logger.getLogger(ReadRelationTest.class.getName()).log(Level.SEVERE, null, ex);
        }

        RegTest.Utility.validate("testOutput/FullJoinTest.txt", "correctOutput/FullJoinTest.txt", false); // test passes if files are equal
    }
    
    public void fullJoinTest(String r1name, String r2name, int jk1, int jk2) throws Exception {  
            ThreadList.init();
            
//            Connector c1 = new Connector("input1");
//            Connector c2 = new Connector("input2");
//            
//            Connector hb1 = new Connector("HSplit-Bloom1");
//            Connector hb2 = new Connector("HSplit-Bloom2");
//            Connector hb3 = new Connector("HSplit-Bloom3");
//            Connector hb4 = new Connector("HSplit-Bloom4");
//
//            Connector hbf1 = new Connector("HSplit-BFilter1");
//            Connector hbf2 = new Connector("HSplit-BFilter2");
//            Connector hbf3 = new Connector("HSplit-BFilter3");
//            Connector hbf4 = new Connector("HSplit-BFilter4");
//
//            Connector bm1 = new Connector("Bloom-Merge1");
//            Connector bm2 = new Connector("Bloom-Merge2");
//            Connector bm3 = new Connector("Bloom-Merge3");
//            Connector bm4 = new Connector("Bloom-Merge4");
//
//            Connector bmm1 = new Connector("Bloom-MMerge1");
//            Connector bmm2 = new Connector("Bloom-MMerge2");
//            Connector bmm3 = new Connector("Bloom-MMerge3");
//            Connector bmm4 = new Connector("Bloom-MMerge4");
//            
//            Connector mha = new Connector("Merge-HSplitA");
//
//            Connector hsahj1 = new Connector("HSplitA-HJoin1");
//            Connector hsahj2 = new Connector("HSplitA-HJoin2");
//            Connector hsahj3 = new Connector("HSplitA-HJoin3");
//            Connector hsahj4 = new Connector("HSplitA-HJoin4");
//            
//            Connector mmms = new Connector("MMerge-MSplit");
//
//            Connector msbf1 = new Connector("MSplit-BFilter1");
//            Connector msbf2 = new Connector("MSplit-BFilter2");
//            Connector msbf3 = new Connector("MSplit-BFilter3");
//            Connector msbf4 = new Connector("MSplit-BFilter4");
//
//            Connector bfm1 = new Connector("BFilter-Merge1");
//            Connector bfm2 = new Connector("BFilter-Merge2");
//            Connector bfm3 = new Connector("BFilter-Merge3");
//            Connector bfm4 = new Connector("BFilter-Merge4");
//
//            Connector mhb = new Connector("Merge-HSplitB");
//            
//            Connector hsbhj1 = new Connector("HSplitB-HJoin1");
//            Connector hsbhj2 = new Connector("HSplitB-HJoin2");
//            Connector hsbhj3 = new Connector("HSplitB-HJoin3");
//            Connector hsbhj4 = new Connector("HSplitB-HJoin4");
//
//            Connector hjm1 = new Connector("HJoin-Merge1");
//            Connector hjm2 = new Connector("HJoin-Merge2");
//            Connector hjm3 = new Connector("HJoin-Merge3");
//            Connector hjm4 = new Connector("HJoin-Merge4");
//            
//            Connector o = new Connector("Merge-Print");
//
//            //-----
//            ReadRelation r1 = new ReadRelation(r1name, c1); 
//            ReadRelation r2 = new ReadRelation(r2name, c2);
//            
//            HSplit hsplit1 = new HSplit(c1, hb1, hb2, hb3, hb4, jk1);
//            HSplit hsplit2 = new HSplit(c2, hbf1, hbf2, hbf3, hbf4, jk2);
//
//            Bloom bloom1 = new Bloom(hb1, bm1, bmm1, jk1);
//            Bloom bloom2 = new Bloom(hb2, bm2, bmm2, jk1);
//            Bloom bloom3 = new Bloom(hb3, bm3, bmm3, jk1);
//            Bloom bloom4 = new Bloom(hb4, bm4, bmm4, jk1);
//
//            Merge merge1 = new Merge(bm1, bm2, bm3, bm4, mha);
//            HSplit hsplit3 = new HSplit(mha, hsahj1, hsahj2, hsahj3, hsahj4, jk1);
//
//            MergeM mmerge1 = new MergeM(bmm1, bmm2, bmm3, bmm4, mmms);
//            SplitM splitm1 = new SplitM(mmms, msbf1, msbf2, msbf3, msbf4);
//
//            BFilter bfilter1 = new BFilter(msbf1, hbf1, bfm1, jk2);
//            BFilter bfilter2 = new BFilter(msbf2, hbf2, bfm2, jk2);
//            BFilter bfilter3 = new BFilter(msbf3, hbf3, bfm3, jk2);
//            BFilter bfilter4 = new BFilter(msbf4, hbf1, bfm4, jk2);
//
//            Merge merge2 = new Merge(bfm1, bfm2, bfm3, bfm4, mhb);
//            HSplit hsplit4 = new HSplit(mhb, hsbhj1, hsbhj2, hsbhj3, hsbhj4, jk2);
//
//            HJoin hjoin1 = new HJoin(hsahj1, hsbhj1, jk1, jk2, hjm1);
//            HJoin hjoin2 = new HJoin(hsahj2, hsbhj2, jk1, jk2, hjm2);
//            HJoin hjoin3 = new HJoin(hsahj3, hsbhj3, jk1, jk2, hjm3);
//            HJoin hjoin4 = new HJoin(hsahj4, hsbhj4, jk1, jk2, hjm4);
//
//            Merge merge3 = new Merge(hjm1, hjm2, hjm3, hjm4, o);
//
//            Print print = new Print(o);
//            ThreadList.run(print);
            

            Connector c1 = new Connector("input1");
            Connector c2 = new Connector("input2");
            ReadRelation r1 = new ReadRelation(r1name, c1); 
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
            
            Merge finalMerge = new Merge(joinConnector[0],joinConnector[1],joinConnector[2],joinConnector[3],o);
            Print p = new Print(o);
            ThreadList.run(p);

            
            
    }
}
