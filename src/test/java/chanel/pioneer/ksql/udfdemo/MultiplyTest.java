package chanel.pioneer.ksql.udfdemo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class MultiplyTest {

	private Multiply multiply = new Multiply();
	
	@Test
	public void shouldMultiplyIntValues() {
		assertEquals(multiply.multiply(2, 3),6);
	}

}
