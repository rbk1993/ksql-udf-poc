package chanel.pioneer.ksql.udfdemo;

import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

public class ArrayOfStructContainsTest {

	private ArrayOfStructContainsRegion jsonUdf = new ArrayOfStructContainsRegion();
	
    @Test
    public void shouldFindIntegersInJsonArray() {
    	
		/*
		 * final String json = "[" +
		 * "{\"REGIONID\":\"region1\",\"DIVISIONID\":\"division1\",\"MARKETID\":\"market1\"},"
		 * +
		 * "{\"REGIONID\":\"region2\",\"DIVISIONID\":\"division1\",\"MARKETID\":\"market1\"},"
		 * +
		 * "{\"REGIONID\":\"region3\",\"DIVISIONID\":\"division1\",\"MARKETID\":\"market1\"}"
		 * + "]";
		 */
        
        List<Struct> jsonArray = new ArrayList<Struct>();
        Schema schema = new Schema(
        		new Field("REGIONID", Type.STRING), 
        		new Field("DIVISIONID", Type.STRING), 
        		new Field("MARKETID", Type.STRING)
        		);
        Struct struct1 = new Struct(schema);
        struct1.set("REGIONID", "region1");
        struct1.set("DIVISIONID", "division1");
        struct1.set("MARKETID", "market1");
        
        Struct struct2 = new Struct(schema);
        struct2.set("REGIONID", "region2");
        struct2.set("DIVISIONID", "division2");
        struct2.set("MARKETID", "market2");
        
        Struct struct3 = new Struct(schema);
        struct3.set("REGIONID", "region3");
        struct3.set("DIVISIONID", "division3");
        struct3.set("MARKETID", "market3");
        
        jsonArray.add(struct1);
        jsonArray.add(struct2);
        jsonArray.add(struct3);
        
        System.out.println(jsonArray.toString());
        
        assertEquals(true, jsonUdf.containsRegion(jsonArray, "region1"));
        assertEquals(true, jsonUdf.containsRegion(jsonArray, "region2"));
        assertEquals(false, jsonUdf.containsRegion(jsonArray, "region4"));
    }
    
}
