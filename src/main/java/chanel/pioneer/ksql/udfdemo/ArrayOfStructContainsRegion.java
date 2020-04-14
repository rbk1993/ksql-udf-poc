package chanel.pioneer.ksql.udfdemo;

import java.util.List;

import org.apache.kafka.common.protocol.types.Struct;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "arrayOfStructContainsRegion", description = "checks if array of struct contains a value")
public class ArrayOfStructContainsRegion {
	
	  public final String REGION_ID = "REGIONID";
	  public final String DIVISION_ID = "DIVISIONID";
	  public final String MARKET_ID = "MARKETID";

	  @Udf(description = "checks if an array of struct contains a region")
	  public <T> Boolean containsRegion(
			  @UdfParameter(value = "markets", schema = "ARRAY<STRUCT<REGIONID VARCHAR, DIVISIONID VARCHAR, MARKETID VARCHAR>>") final List<Struct> markets,
			  @UdfParameter("region") final String region
	  ) {
		  for(int i=0;i<markets.size();i++) {
			  System.out.println("markets["+i+"]->REGIONID = "+markets.get(i).getString("REGIONID"));
			  if(markets.get(i).getString("REGIONID").equals(region)) return true;
		  }
		  return false;
	  }
	  
}
