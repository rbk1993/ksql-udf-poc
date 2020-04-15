package chanel.pioneer.ksql.udfdemo;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
        
        Schema marketsSchema = SchemaBuilder.struct()
                .field("REGIONID", Schema.STRING_SCHEMA)
                .field("DIVISIONID", Schema.STRING_SCHEMA)
                .field("MARKETID", Schema.STRING_SCHEMA)
                .build();
        
        Struct struct1 = new Struct(marketsSchema)
        .put("REGIONID", "region1")
        .put("DIVISIONID", "division1")
        .put("MARKETID", "market1");
        
        Struct struct2 = new Struct(marketsSchema)
        .put("REGIONID", "region2")
        .put("DIVISIONID", "division2")
        .put("MARKETID", "market2");
        
        Struct struct3 = new Struct(marketsSchema)
        .put("REGIONID", "region3")
        .put("DIVISIONID", "division3")
        .put("MARKETID", "market3");
        
        jsonArray.add(struct1);
        jsonArray.add(struct2);
        jsonArray.add(struct3);
        
        System.out.println(jsonArray.toString());
        
        assertEquals(false, jsonUdf.containsRegion(null, null));
        assertEquals(false, jsonUdf.containsRegion(jsonArray, null));
        assertEquals(true, jsonUdf.containsRegion(jsonArray, "region1"));
        assertEquals(true, jsonUdf.containsRegion(jsonArray, "region2"));
        assertEquals(true, jsonUdf.containsRegion(jsonArray, "region3"));
        assertEquals(false, jsonUdf.containsRegion(jsonArray, "region4"));
    }
    
}
