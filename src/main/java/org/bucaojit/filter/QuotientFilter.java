/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bucaojit.filter;

// Author: Oliver

// General purpose quotient filter, takes in java Objects as an entry 
// Approximate Membership Query (AMQ)

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.BasicConfigurator;

public class QuotientFilter {
    private static final Log LOG = LogFactory.getLog(QuotientFilter.class);
	private static final int DEFAULT_SIZE = 1000;
    private int qfSize;
	private Slot[] set;
	protected int capacity;
	
	public QuotientFilter() {
	    this(DEFAULT_SIZE);
	}
	
	public QuotientFilter(int size) {
	    LOG.info("Created QuotientFilter of size: " + size);
		this.set = new Slot[size];
		for(int i = 0; i < size; i++) 
			this.set[i] = new Slot();
		this.capacity = size;
        this.qfSize = 0;
	}
	
	public int getCapacity() {
		return this.capacity;
	}

    public int getSize() {
        return this.qfSize;
    }
    
    public boolean isFull() {
    	return getSize() >= getCapacity();
    }
	
	public void setSlot(int index, Slot slot) {
		this.set[index] = slot;
	}

	public void insert(long hash) throws Exception{
		
		if(isFull()) {
			throw new IOException("ERROR: Quotient Filter has reached capacity");
		}
		int index = Utils.getIndex(hash, getCapacity());
		Slot currentSlot = set[index];
	    int remainder = Utils.getRemainder(hash);
        
		if(!currentSlot.getMetadata().getOccupied()) {		
			currentSlot.setRemainder(Utils.getRemainder(hash));

			Metadata md = new MetadataBitSet();
			md.setOccupied();
			currentSlot.setMetadata(md);
			currentSlot.setRemainder(remainder);
		}
		else { 
			int foundIndex;
			foundIndex = lookup(index, remainder);
			if(foundIndex != -1) { 
				currentSlot.increase();
			}
			else {
				insertShift(remainder, index);
			}
		}
	}

    public void insertShift(int remainder, int index) throws IOException {
        Integer runStart = 0;
        Integer position = index;
        boolean atStart = true;
        boolean subtract = false;
    	Metadata md = new MetadataBitSet();
        md.setOccupied();
        Slot newSlot = new Slot(remainder, md);
        
        runStart = findRunStart(index);
        Slot currentSlot = set[runStart];
        while ((remainder > currentSlot.getRemainder()) && currentSlot.getMetadata().getOccupied()) {
        	atStart = false;
        	position++;
        	subtract = true;
        	currentSlot = set[position];
        }
        
        if(subtract)
        	position--;
        Slot prevSlot = set[position];
        if(prevSlot.getMetadata().getShifted())
        	newSlot.getMetadata().setShifted();
        
        shiftRight(position);
        
        if(!atStart) {
        	newSlot.getMetadata().setContinuation();
        }
        set[position] = newSlot;
    }
    
    public void shiftRight(int index) {
    	Slot currentSlot;
    	Slot nextSlot;
    	Slot temp = null;
    	boolean setContinuation = true;
    	
    	do { 
    		currentSlot = set[index % getCapacity()];
    		nextSlot = set[(index+1) % getCapacity()];
    		temp = nextSlot;
    		// nextSlot = currentSlot;
    		currentSlot.getMetadata().setShifted();
    		currentSlot.getMetadata().setContinuation();
    		set[index+1 % getCapacity()] = currentSlot;
    		
    		index++;
    	} while (set[index+1 % getCapacity()].getMetadata().getOccupied());
    	
    	if(temp.getMetadata().getOccupied()) {
    		set[index % getCapacity()] =  temp;
    	}
    }

    public void deleteShift(int index) throws IOException {       
    	Slot slot, nextSlot; 
    	do {
    		slot = set[index];
    		nextSlot = set[(index++) % getCapacity()];
    		slot = nextSlot;
    	}while(!slot.getMetadata().getOccupied());
    }
	
	public void delete(long hash) throws Exception {
		int index = Utils.getIndex(hash, getCapacity());
		int foundIndex;
		foundIndex = lookup(index, Utils.getRemainder(hash));
		if(foundIndex != -1) {
			// No slots to move, inserting empty slot
			if(!set[(foundIndex+1) % getCapacity()].getMetadata().getOccupied()) {
				Slot newSlot = new Slot();
				set[foundIndex] =  newSlot;
			}
			deleteShift(foundIndex);
		}
		else {
			LOG.debug("Unable to delete, no hash: " + hash);
		}
	}

	public int getNumberOfOccurences(long hash) {
		int index = lookup(hash);

		return index == -1 ? 0 : set[index].getCount();
	}

	private int lookup(long hash) {
		return lookup(Utils.getIndex(hash, getCapacity()), Utils.getRemainder(hash));
	}
	
	int lookup(int index, int remainder) {
		int currentIndex = index;
		Slot currentSlot = set[currentIndex];
        int runStart = 0;
        
		if(currentSlot.getMetadata().isClear())
			return -1;
		
		runStart = findRunStart(currentIndex);
		
		return checkQuotient(runStart, remainder);	
	}
	
	private int checkQuotient(int runStart, int remainder) {
		int currentIndex = runStart;
		Slot slot = set[runStart];
		
		do {
			if (slot.getRemainder() == remainder) {
				return currentIndex;
			}
			else if(slot.getRemainder() > remainder) {
				return -1;
			}
			currentIndex++;
			if(currentIndex >= getCapacity()) 
				currentIndex = 0;
			slot = set[currentIndex];
		} while(slot.getMetadata().getContinuation());
		
		// Did not find the remainder in the run, false
		return -1;
	}
	
	private int findRunStart(int currentIndex) {
		int isOccupiedCount = 0;
		int isContinuationCount = 0;
		Slot slot = new Slot(); 
		
		while (true) {
			slot = set[currentIndex];
			if(slot.getMetadata().getOccupied()) 
				isOccupiedCount++;
			if(!slot.getMetadata().getShifted()) 
				break;
			currentIndex--;
			if(currentIndex < 0) 
				currentIndex = getCapacity() - 1;
		}
		
		// currentIndex is now the start of the CLUSTER
		while(true) {
			slot = set[currentIndex];
			if (!slot.getMetadata().getContinuation()) 
				isContinuationCount++;
			if(isOccupiedCount <= isContinuationCount) {
				return currentIndex;
			}
			currentIndex++;
			if(currentIndex > (getCapacity() - 1))
				currentIndex = 0;
		}
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();
		boolean first = true;
		int count = 0;
		for(Slot slot : set) {


			if((!slot.getMetadata().getOccupied())) {
				sb.append("<empty>");
			}
			else {
				sb.append(slot.getMetadata().getOccupied() ? "1" : "0");
				sb.append(slot.getMetadata().getContinuation() ? "1" : "0");
				sb.append(slot.getMetadata().getShifted() ? "1" : "0");
				sb.append(":");
				sb.append(slot.getRemainder());
				sb.append(" ");
			}
			count++;
			if((count % 15) == 0){
				sb.append("\n");
			}
		}
		return sb.toString();
	}
	
	public void printQF() {
		System.out.println(this.toString() + "\n");
	}
	
	public static void main(String[] args) throws Exception{
	    BasicConfigurator.configure();
		QuotientFilter qf = new QuotientFilter(45);
		LOG.error("ERROR logging");
		System.out.println(qf.hashCode());

		long value = 222;
		long stringInput = "hello world".hashCode();
		long longval = 4444;
		
		qf.insert(value);
		System.out.println(qf.toString());
		System.out.println("");
		qf.insert(stringInput);
		System.out.println(qf.toString());
		System.out.println("");
		qf.insert(longval);
		
		
		System.out.println(qf.toString());
		
	}	
}
