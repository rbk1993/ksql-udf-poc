package chanel.pioneer.ksql.udfdemo;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(name = "helloworld", description = "Shows hello world with a name")
public class Helloworld {

  @Udf(description = "Shows hello world with a String name")
  public String helloworld(
		  @UdfParameter("name") String name) {
    return "Hello world, "+name;
  }

}
