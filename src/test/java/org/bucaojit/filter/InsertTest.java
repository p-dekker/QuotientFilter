package org.bucaojit.filter;

import junit.framework.Assert;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;


public class InsertTest extends TestCase{
	
	public InsertTest( String testName )
    {
        super( testName );
    }
	
    public static Test suite()
    {
        return new TestSuite( InsertTest.class );
    }
	
	public  void testApp() {
		QuotientFilter qf = new QuotientFilter(75);
		try {
			qf.insert("value".hashCode());
			qf.printQF();
			qf.insert("second value".hashCode());
			qf.printQF();
			qf.insert(343443L);
			qf.printQF();
			qf.insert(343443L);
			qf.printQF();
			qf.insert(444L);
			qf.printQF();
			qf.insert(23L);
			qf.printQF();

			System.out.println("Checking for 'value'");
			int output = qf.getNumberOfOccurences("value".hashCode());
			System.out.println("The value output: " + output);
			Assert.assertEquals(1, output);
			
			int outputSecond = qf.getNumberOfOccurences(23L);
			System.out.println("The 23 output: " + outputSecond);
			Assert.assertEquals(1, outputSecond);
			
			int outputThird = qf.getNumberOfOccurences(444L);
			System.out.println("The 4444 output: " + outputThird);
			Assert.assertEquals(1, outputThird);
			
			int outputFourth = qf.getNumberOfOccurences("Not Exists".hashCode());
			System.out.println("The 'Not Exists' output: " + outputFourth);
			Assert.assertEquals(0, outputFourth);

			int outputFifth= qf.getNumberOfOccurences(343443L);
			System.out.println("The 44343443 output: " + outputFifth);
			Assert.assertEquals(2, outputFifth);

			
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	public static void main(String str[]) {
		InsertTest it = new InsertTest("insert test");
		it.testApp();
	}
}
