package edu.coursera.distributed;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
public final class PageRank {
    private PageRank() {
    }
    public static JavaPairRDD<Integer, Double> sparkPageRank(
            final JavaPairRDD<Integer, Website> sites,
            final JavaPairRDD<Integer, Double> ranks) {
        return sites
                .join(ranks)
                .flatMapToPair(kv -> {
                    Website edges = kv._2()._1();
                    Double currentRank = kv._2()._2();

                    List<Tuple2<Integer, Double>> contribs = new LinkedList<>();
                    Iterator<Integer> iter = edges.edgeIterator();
                    while (iter.hasNext()) {
                        final int target = iter.next();
                        contribs.add(new Tuple2<>(target, currentRank / (double) edges.getNEdges()));
                    }
                    return contribs;
                })
                .reduceByKey((Double d1, Double d2) -> d1 + d2)
                .mapValues(v -> 0.15 + 0.85 * v);

    }
}