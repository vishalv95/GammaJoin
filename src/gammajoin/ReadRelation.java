package gammajoin;

import java.util.StringTokenizer;
import gammaSupport.*;
import basicConnector.*;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.Scanner;

public class ReadRelation extends Thread {
    Relation rel;
    StringTokenizer st;
    Connector connection;
    
    public ReadRelation(String relation, Connector c){
        connection = c;
        try {
            //Read data from file to tokenizer
            String textData = new Scanner(new File(relation + ".txt") ).useDelimiter("\\Z").next();
            st = new StringTokenizer(textData, "\n");

            //Read first line for field names
            String[] fieldNames = st.nextToken().split(" ");
            
            //Initialize table
            rel = new Relation(relation, fieldNames.length);
            
            //Add field names
            for (String field: fieldNames)
                rel.addField(field);
            
            //Flush the separator
            st.nextToken();
            
            c.setRelation(rel);
            
            ThreadList.add(this);
        } 
        
        //Handle exceptions
        catch (FileNotFoundException ex) {
            Logger.getLogger(ReadRelation.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    public void run(){
            //Print the rest of the tuples to the write end
            while (st.hasMoreTokens()){
                Tuple tup = Tuple.makeTupleFromFileData(rel, st.nextToken());
                try {
                    connection.getWriteEnd().putNextTuple(tup);
                } 
                
                catch (Exception ex) {
                    Logger.getLogger(ReadRelation.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        
    }
}