package chanel.pioneer.ksql.udfdemo;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

public class ArrayOfStructContainsRegionTest {

	private ArrayOfStructContainsRegion udf = new ArrayOfStructContainsRegion();
	
    @Test
    public void shouldFindRegionInArrayOfStruct() {
    	
		/*
		 * final String json = "[" +
		 * "{\"REGIONID\":\"region1\",\"DIVISIONID\":\"division1\",\"MARKETID\":\"market1\"},"
		 * +
		 * "{\"REGIONID\":\"region2\",\"DIVISIONID\":\"division2\",\"MARKETID\":\"market2\"},"
		 * +
		 * "{\"REGIONID\":\"region3\",\"DIVISIONID\":\"division3\",\"MARKETID\":\"market3\"}"
		 * + "]";
		 */
        
        List<Struct> arrayOfStruct = new ArrayList<Struct>();
        
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
        
        arrayOfStruct.add(struct1);
        arrayOfStruct.add(struct2);
        arrayOfStruct.add(struct3);
        
        System.out.println(arrayOfStruct.toString());
        
        assertEquals(false, udf.containsRegion(null, null));
        assertEquals(false, udf.containsRegion(arrayOfStruct, null));
        assertEquals(true, udf.containsRegion(arrayOfStruct, "region1"));
        assertEquals(true, udf.containsRegion(arrayOfStruct, "region2"));
        assertEquals(true, udf.containsRegion(arrayOfStruct, "region3"));
        assertEquals(false, udf.containsRegion(arrayOfStruct, "region4"));
    }
    
}
