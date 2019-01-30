package org.bucaojit.filter;

import junit.framework.TestCase;

public class MetadataTest extends TestCase {

    public void testApp() {
        MetadataBitSet md = new MetadataBitSet();

        assertTrue(md.isClear());
        assertFalse(md.getContinuation());
        assertFalse(md.getOccupied());
        assertFalse(md.getShifted());

        md.setContinuation();
        assertFalse(md.isClear());
        assertTrue(md.getContinuation());
        assertFalse(md.getOccupied());
        assertFalse(md.getShifted());

        md.setOccupied();
        assertFalse(md.isClear());
        assertTrue(md.getContinuation());
        assertTrue(md.getOccupied());
        assertFalse(md.getShifted());

        md.setShifted();
        assertFalse(md.isClear());
        assertTrue(md.getContinuation());
        assertTrue(md.getOccupied());
        assertTrue(md.getShifted());

    }
}