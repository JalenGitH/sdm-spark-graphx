package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import models.VertexPayload;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,VertexPayload,VertexPayload,VertexPayload> implements Serializable {
        @Override
        public VertexPayload apply(Long vertexID, VertexPayload vertexValue, VertexPayload message) {
            return message;
//            return null;
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<VertexPayload,Integer>, Iterator<Tuple2<Object,VertexPayload>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, VertexPayload>> apply(EdgeTriplet<VertexPayload, Integer> triplet) {
            Tuple2<Object,VertexPayload> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,VertexPayload> dstVertex = triplet.toTuple()._2();

            VertexPayload sourceVertextPayload = sourceVertex._2;
            VertexPayload dstVertextPayload = dstVertex._2;

            Integer edgeCost = triplet.toTuple()._3();
            if (sourceVertextPayload.value > dstVertextPayload.value) {   // if source shortest path is greater than destination shortest path
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,VertexPayload>>().iterator()).asScala();
            } else {
                // otherwise propagate the message
                sourceVertextPayload.path.add((Long) sourceVertex._1);
                // the message will be source shortest path plus the cost of edge between source and destination
                VertexPayload message = new VertexPayload(sourceVertextPayload.value+edgeCost, sourceVertextPayload.path);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,VertexPayload>(triplet.dstId(), message)).iterator()).asScala();
            }
//            return null;
        }
    }

    private static class merge extends AbstractFunction2<VertexPayload,VertexPayload,VertexPayload> implements Serializable {
        @Override
        public VertexPayload apply(VertexPayload o, VertexPayload o2) {
            if (o.value < o2.value){
                return o;
            } else {
                return o2;
            }
//            return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();
        //here
        List<Tuple2<Object,VertexPayload>> vertices = Lists.newArrayList(
                new Tuple2<>(1l, new VertexPayload(0,new ArrayList<>(Arrays.asList( new Long[] {1l})))),
                new Tuple2<>(2l, new VertexPayload(Integer.MAX_VALUE,new ArrayList<>())),
                new Tuple2<>(3l, new VertexPayload(Integer.MAX_VALUE,new ArrayList<>())),
                new Tuple2<>(4l, new VertexPayload(Integer.MAX_VALUE,new ArrayList<>())),
                new Tuple2<>(5l, new VertexPayload(Integer.MAX_VALUE,new ArrayList<>())),
                new Tuple2<>(6l, new VertexPayload(Integer.MAX_VALUE,new ArrayList<>()))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<>(1l,2l, 4), // A --> B (4)
                new Edge<>(1l,3l, 2), // A --> C (2)
                new Edge<>(2l,3l, 5), // B --> C (5)
                new Edge<>(2l,4l, 10), // B --> D (10)
                new Edge<>(3l,5l, 3), // C --> E (3)
                new Edge<>(5l, 4l, 4), // E --> D (4)
                new Edge<>(4l, 6l, 11) // D --> F (11)
        );

        //here
        JavaRDD<Tuple2<Object,VertexPayload>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        //here
        Graph<VertexPayload,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new VertexPayload(1,new ArrayList<>()), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(VertexPayload.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(VertexPayload.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(new VertexPayload(0, new ArrayList<>()),
                Integer.MAX_VALUE,
                EdgeDirection.Out(),
                new VProg(),
                new sendMsg(),
                new merge(),
                ClassTag$.MODULE$.apply(VertexPayload.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object,VertexPayload> vertex = (Tuple2<Object, VertexPayload>)v;
                    VertexPayload message = vertex._2;
                    ArrayList<Long> mShortestPathInLong = message.path;
//                    ArrayList<String> mShortestPathString = mShortestPathInLong.forEach(x -> labels[x]);
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+message.value+" via "+mShortestPathInLong);
                });
    }

}
