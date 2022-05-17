package flinkdemo.util.visual;

import com.google.common.geometry.S2LatLng;
import flinkdemo.entity.Point;
import flinkdemo.util.TopologyGraph;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Stream;

public class JdbcConnection {
    private final Connection conn;

    public static void main(String[] args) throws Exception {
        JdbcConnection jdbcConnection = new JdbcConnection();
        jdbcConnection.defineClusterTable();
//        jdbcConnection.addPoint();
//        jdbcConnection.addLine();
        jdbcConnection.close();
    }

    public JdbcConnection() throws Exception {
        String url = "jdbc:postgresql://localhost:5432/roadbenchmark";
        String user = "postgres";
        String password = "postgres";

        Class.forName("org.postgresql.Driver");
        this.conn = DriverManager.getConnection(url, user, password);
    }

    public void defineTable() throws Exception {
        Statement state = conn.createStatement();

        String sql = "CREATE TABLE background (" +
                "ID serial PRIMARY KEY," +
                "geom geometry(LINESTRING, 4326)" +
                ");";
        state.executeUpdate(sql);
        state.close();
    }

    /**
     * 我们的数据库表已经修改为存储线对象，此函数已经废弃
     */
    public void addPoint() throws Exception {
        String coDataFileName = "F:\\\\road network benchmark\\\\USA-road-d.NY.co\\\\USA-road-d.NY.co";
        Stream<String> coLines = Files.lines(Paths.get(coDataFileName));
        Statement state = conn.createStatement();

        coLines.forEach(ele -> {
            String[] arr = ele.split("\\s+");
            if (arr[0].equals("v")) {
                // 坐标数据集中，坐标精确到6位小数，直接使用fromE6最快
                S2LatLng s2LatLng = S2LatLng.fromE6(Integer.parseInt(arr[3]), Integer.parseInt(arr[2]));
                try {
                    String sql = "INSERT INTO background(dataIndex, geom) VALUES (1, 'POINT(" + s2LatLng.lngDegrees()
                            + " " + s2LatLng.latDegrees() + ")');";
                    state.executeUpdate(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        state.close();
    }

    public void addLine() throws Exception {
        String coDataFileName = "F:\\\\road network benchmark\\\\USA-road-d.NY.co\\\\USA-road-d.NY.co";
        String grDataFileName = "F:\\\\road network benchmark\\\\USA-road-d.NY.gr\\\\USA-road-d.NY.gr";
        Stream<String> coLines = Files.lines(Paths.get(coDataFileName));
        Stream<String> grLines = Files.lines(Paths.get(grDataFileName));
        Statement state = conn.createStatement();

        coLines.forEach(ele -> {
            String[] arr = ele.split("\\s+");
            if (arr[0].equals("v")) {
                // 坐标数据集中，坐标精确到6位小数，直接使用fromE6最快
                S2LatLng s2LatLng = S2LatLng.fromE6(Integer.parseInt(arr[3]), Integer.parseInt(arr[2]));
                TopologyGraph.insertVertex(arr[1], s2LatLng);
            }
        });

        grLines.forEach(ele -> {
            String[] arr = ele.split("\\s+");
            if (arr[0].equals("a")) {
                S2LatLng source = new S2LatLng(TopologyGraph.getVertex(arr[1]));
                S2LatLng target = new S2LatLng(TopologyGraph.getVertex(arr[2]));
                try {
                    String sql = "INSERT INTO background(geom) VALUES ('LINESTRING(" + source.lngDegrees() + " "
                            + source.latDegrees() + "," + target.lngDegrees() + " "
                            + target.latDegrees() + ")')";
                    state.executeUpdate(sql);
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        state.close();
    }

    public void defineClusterTable() throws Exception {
        Statement state = conn.createStatement();

        String sql = "CREATE TABLE cluster (" +
                "ID serial PRIMARY KEY," +
                "clusterID int4," +
                "geom geometry(LINESTRING, 4326)" +
                ");";
        state.executeUpdate(sql);
        state.close();
    }

    public void addCluster(int clusterID, List<Point> convexHull) throws Exception {
        Statement state = conn.createStatement();
        StringBuffer sb = new StringBuffer("INSERT INTO cluster(clusterID, geom) VALUES ("
                + clusterID + ",'LINESTRING(");

        for (Point point : convexHull) {
            sb.append(point.x).append(" ").append(point.y).append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")')");
        try {
            state.executeUpdate(sb.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        state.close();
    }

    public void close() throws Exception {
        conn.close();
    }
}
