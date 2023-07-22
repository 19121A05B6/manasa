package edu.coursera.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
public final class ReciprocalArraySum {
    private ReciprocalArraySum() {
    }
    protected static double seqArraySum(final double[] input) {
        double sum = 0;
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }
        return sum;
    }
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }
    private static int getChunkStartInclusive(final int chunk,
                                              final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
                                            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }
    private static class ReciprocalArraySumTask extends RecursiveAction {
        private final int startIndexInclusive;
        private final int endIndexExclusive;
        private final double[] input;
        private double value;
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                               final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            int THRESHOLD = 1000;
            if (endIndexExclusive - startIndexInclusive <= THRESHOLD) {
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    value += 1 / input[i];
                }
            } else {
                int half = (startIndexInclusive + endIndexExclusive) / 2;
                ReciprocalArraySumTask left = new ReciprocalArraySumTask(startIndexInclusive, half, input);
                ReciprocalArraySumTask right = new ReciprocalArraySumTask(half, endIndexExclusive, input);
                left.fork();
                right.compute();
                left.join();
                value += left.getValue() + right.getValue();
            }
        }
    }
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;


        ReciprocalArraySumTask t = new ReciprocalArraySumTask(0, input.length, input);
        ForkJoinPool.commonPool().invoke(t);
        return t.getValue();
    }
    protected static double parManyTaskArraySum(final double[] input,
                                                final int numTasks) {


        System.setProperty("java.util.concurrent.ForkJoinPool.common.parallelism", String.valueOf(4));
        List<ReciprocalArraySumTask> reciprocalArraySumTasks = new ArrayList<>();
        double sum = 0;

        for (int i = 0; i < numTasks; i++) {
            int start = getChunkStartInclusive(i, numTasks, input.length);
            int end = getChunkEndExclusive(i, numTasks, input.length);

            ReciprocalArraySumTask t = new ReciprocalArraySumTask(start, end, input);
            ForkJoinPool.commonPool().invoke(t);
            sum += t.getValue();
        }
        return sum;
    }
}