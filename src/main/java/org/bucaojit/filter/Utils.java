package org.bucaojit.filter;

public class Utils {
	
	public static int getQuotient(long hash) {
		hash &= Long.MAX_VALUE;
		return (int) (hash >>> 32);
	}
	
	public static int getRemainder(long hash) {
		return (int) hash;
	}

	public static int getIndex(long hash, int size) {
		return getQuotient(hash) % size;
	}

}
