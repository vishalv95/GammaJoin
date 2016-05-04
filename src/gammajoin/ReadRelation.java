package gammajoin;

import java.util.StringTokenizer;
import gammaSupport.*;
import basicConnector.*;
import java.io.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ReadRelation extends Thread {
    Relation rel;
    BufferedReader relReader;
    Connector connection;
    
    public ReadRelation(String relation, Connector c) throws IOException{
        connection = c;
        try {
            //Read data from file to tokenizer
            relReader = new BufferedReader(new FileReader(relation + ".txt"));
            String textData = relReader.readLine();
            
            String[] fieldNames = textData.split("\\s+");
            
//#            StringTokenizer st = new StringTokenizer(textData);
            
            //Initialize table
            rel = new Relation(relation, fieldNames.length);
            
            //Add field names
            for (String field: fieldNames)
                rel.addField(field);
            
//#            while(st.hasMoreTokens()){
//#                rel.addField(st.nextToken());
//#            }
            
            c.setRelation(rel);
            
            //Flush the separator
            relReader.readLine();
            
            ThreadList.add(this);
        } 
        
        //Handle exceptions
        catch (FileNotFoundException ex) {
            Logger.getLogger(ReadRelation.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
    
    public void run(){
            //Print the rest of the tuples to the write end
            String line;
            try {
                while (true){    
                    line = relReader.readLine();
                    if(line == null) break;
                    Tuple tup = Tuple.makeTupleFromFileData(rel, line);
                    connection.getWriteEnd().putNextTuple(tup);
                }
            }
            
            catch (Exception ex) {
                    Logger.getLogger(ReadRelation.class.getName()).log(Level.SEVERE, null, ex);
            } 
    }
}