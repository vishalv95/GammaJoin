/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gammajoin;

import java.util.StringTokenizer;
import basicConnector.*;
/**
 *
 * @author Vishal
 */
public class ReadRelation extends Thread {
    public ReadRelation(String relation, Connector c){
        c.getReadEnd();
    }
}
