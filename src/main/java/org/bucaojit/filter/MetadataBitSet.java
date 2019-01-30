package org.bucaojit.filter;

import java.util.BitSet;

public class MetadataBitSet implements Metadata{
	private final int OCCUPIED_BIT = 1;
	private final int CONTINUATION_BIT = 2;
	private final int SHIFTED_BIT = 4;
	
	private byte metadata;
	
	public MetadataBitSet() {
		this.metadata = 0;
	}
	
	public MetadataBitSet(MetadataBitSet metadata) {
		this.metadata = metadata.metadata;
	}
	
	public boolean getOccupied() { return (metadata & OCCUPIED_BIT) == OCCUPIED_BIT;
	}
	
	public boolean getShifted() {
		return (metadata & SHIFTED_BIT) == SHIFTED_BIT;
	}
	
	public boolean getContinuation() {
		return (metadata & CONTINUATION_BIT) == CONTINUATION_BIT;
	}

	public void setOccupied() {
		metadata = (byte) (metadata | OCCUPIED_BIT);
	}
	
	public void setContinuation() {
		metadata = (byte) (metadata | CONTINUATION_BIT);
	}
	
	public void setShifted() {
		metadata = (byte) (metadata | SHIFTED_BIT);
	}
	
	public void clearOccupied() {
		metadata = (byte) (metadata & (~OCCUPIED_BIT));
	}
	
	public void clearContinuation() {
		metadata = (byte) (metadata & (~CONTINUATION_BIT));
	}
	
	public void clearShifted() {
		metadata = (byte) (metadata & (~SHIFTED_BIT));
	}
	
	public boolean isClear() {
		return (metadata & (OCCUPIED_BIT + CONTINUATION_BIT + SHIFTED_BIT)) == 0;
	}

}
