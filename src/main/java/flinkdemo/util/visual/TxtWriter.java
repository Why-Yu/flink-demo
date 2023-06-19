package flinkdemo.util.visual;

import com.google.common.geometry.S2LatLng;
import flinkdemo.entity.NextNode;
import flinkdemo.entity.Vertex;
import flinkdemo.util.TopologyGraph;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class TxtWriter {
    public static void cutData(String coFileName, String grFileName) throws IOException {
        File coFile = new File(coFileName);
        if(!coFile.exists()){
            coFile.createNewFile();
        }
        FileOutputStream coOutputStream = new FileOutputStream(coFile);
        TreeSet<String> treeSet = new TreeSet<>();
        HashMap<String, Integer> old2newIndex = new HashMap<>();
        int dataIndex = 1;

        for(Vertex vertex : TopologyGraph.getVertices()) {
            S2LatLng s2LatLng = new S2LatLng(vertex);
            double lng = s2LatLng.lngDegrees();
            double lat = s2LatLng.latDegrees();
            if (lng < -73.92 && lat > 40.72) {
                treeSet.add(vertex.dataIndex);
                int lngInt = (int) (lng * 1000000);
                int latInt = (int) (lat * 1000000);
                String coMessage = "v " + dataIndex + " " +
                        lngInt + " " + latInt + "\r\n";
                coOutputStream.write(coMessage.getBytes(StandardCharsets.UTF_8));
                old2newIndex.put(vertex.dataIndex, dataIndex);
                dataIndex++;
            }
        }
        coOutputStream.flush();
        coOutputStream.close();

        File grFile = new File(grFileName);
        if(!grFile.exists()){
            grFile.createNewFile();
        }
        FileOutputStream grOutputStream = new FileOutputStream(grFile);
        Map<String, Integer> index = TopologyGraph.getDataIndexToVerticesIndex();
        List<Vertex> vertices = TopologyGraph.getVertices();
        for(Vertex vertex : TopologyGraph.getVertices()) {
            if(treeSet.contains(vertex.dataIndex)) {
                int verticesIndex = index.get(vertex.dataIndex);
                NextNode current = vertices.get(verticesIndex).nextNode;
                while(current != null){
                    if(treeSet.contains(current.dataIndex)) {
                        int newFrom = old2newIndex.get(vertex.dataIndex);
                        int newTo = old2newIndex.get(current.dataIndex);
                        String grMessage = "a " + newFrom + " " + newTo +
                                " " + current.weight + "\r\n";
                        grOutputStream.write(grMessage.getBytes(StandardCharsets.UTF_8));
                    }
                    current = current.nextNode;
                }
            }
        }
        grOutputStream.flush();
        grOutputStream.close();
    }
}
