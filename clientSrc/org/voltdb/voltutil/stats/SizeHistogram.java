package org.voltdb.voltutil.stats;

/* This file is part of VoltDB.
 * Copyright (C) 2024 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * Stores a histogram of 'something', where buckets can be incremented by
 * arbitrary amounts.
 */
public class SizeHistogram {
    
     final String NUMFORMAT_INTEGER = "%16d";

    String description = "";
    String name = "";
    
    /**
     * Highest value seen
     */
    int maxUsedSize = 0;

    int[] theHistogram = new int[0];
    String[] theHistogramComment = new String[0];

    public SizeHistogram(String name, int size) {
        this.name = name;
        theHistogram = new int[size];
        theHistogramComment = new String[size];
    }

    public void inc(int size, String comment) {

        if (size >= 0 && size < theHistogram.length) {
            theHistogram[size]++;
            theHistogramComment[size] = comment;
        } else if (size >= theHistogram.length) {
            theHistogram[theHistogram.length - 1]++;
            theHistogramComment[theHistogram.length - 1] = comment;
        }
        
        if (maxUsedSize < size) {
            maxUsedSize = size;
        }

    
    }
    
    

    @Override
    public String toString() {
        StringBuffer b = new StringBuffer(name);
        b.append(" ");
        b.append(description);
        b.append(" ");

        for (int i = 0; i < theHistogram.length; i++) {
            if (theHistogram[i] > 0) {
                b.append(System.lineSeparator());
                b.append(i);
                b.append(' ');
                b.append(theHistogram[i]);

            }
        }

        return b.toString();
    }
    
    public String toStringShort() {
        StringBuffer b = new StringBuffer(name);

        b.append(" ");
        b.append(description);
        b.append(System.lineSeparator());

        b.append(" Reports=");
        b.append(String.format(NUMFORMAT_INTEGER, getEventTotal()));
         b.append(", 50%=");
        b.append(String.format(NUMFORMAT_INTEGER, getSizePct(50)));
        b.append(", 95%=");
        b.append(String.format(NUMFORMAT_INTEGER, getSizePct(95)));
        b.append(", 99%=");
        b.append(String.format(NUMFORMAT_INTEGER, getSizePct(99)));
        b.append(", 99.5%=");
        b.append(String.format(NUMFORMAT_INTEGER, getSizePct(99.5)));
        b.append(", 99.95%=");
        b.append(String.format(NUMFORMAT_INTEGER, getSizePct(99.95)));
        b.append(", Max=");
        b.append(String.format(NUMFORMAT_INTEGER, maxUsedSize));

          
       return b.toString();
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
  
    public int getSizePct(double pct) {

        final double target = getEventTotal() * (pct / 100);
        double runningTotal = theHistogram[0];
        int matchValue = 0;

        for (int i = 1; i < theHistogram.length; i++) {

            if (runningTotal >= target) {
                break;
            }

            matchValue = i;
            runningTotal = runningTotal + theHistogram[i];

        }

        return matchValue;
    }

   
  
    
    public long getEventTotal() {

        long runningTotal = 0;

        for (double element : theHistogram) {
            runningTotal += element;
        }

        return runningTotal;
    }

    /**
     * @return the maxUsedSize
     */
    public int getMaxUsedSize() {
        return maxUsedSize;
    }

    /**
     * @return the theHistogramComment
     */
    public String getTheHistogramComment(int commentId) {
        return theHistogramComment[commentId];
    }


}
