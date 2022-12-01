import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.io.*;
//import java.sql.*;


public class Assignment1 {
    private Connection conn =null;
    private Integer mid = 1;

    /**
     * Constructor
     */
    public Assignment1(String ConnectionString, String DBUsername, String DBPassword) {
        super();
        connect(ConnectionString, DBUsername, DBPassword);
    }
    /**
     * The function makes the connection to the DB
     */


    private void connect(String ConnectionString, String DBUsername, String DBPassword){
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            //String connectionUrl = "jdbc:sqlserver://132.72.64.124:1433;databaseName=amitpart;user=amitpart;password=G7DkEb2Y;encrypt=false;";
            String connectionUrl = "jdbc:sqlserver://132.72.64.124:1433;databaseName="+ConnectionString+";user="+DBUsername+";password="+DBPassword+";encrypt=false;";
            this.conn = DriverManager.getConnection(connectionUrl);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * The function closes the connection to the DB
     */
    private void disconnect(){
        try {
            this.conn.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void fileToDataBase(String path){
        if(this.conn==null){
            //connect();
        }
        int batchSize = 20;

        //conn.setAutoCommit(false);
        PreparedStatement ps = null;
        try{

            String query = "INSERT INTO MEDIAITEMS(MID,TITLE,PROD_YEAR, TITLE_LENGTH)"+ " VALUES(?,?,?,?)"; //query
            ps = conn.prepareStatement(query); //compiling query in the DB


            BufferedReader lineReader = new BufferedReader(new FileReader(path));
            String lineText = null;

            int count = 0;

            lineReader.readLine(); // skip header line

            while ((lineText = lineReader.readLine()) != null) {
                //System.out.println(lineText);
                String[] data = lineText.split(",");
               // String mid = ;
                String title = data[0];
                String prodYear = data[1];
                Integer titleLength = title.length();


                ps.setInt(1, this.mid);
                this.mid++;
                ps.setString(2, title);



                int prodYear1 = Integer.parseInt(prodYear);
                ps.setInt(3, prodYear1);

                ps.setInt(4, titleLength);

                ps.addBatch();

                if (count % batchSize == 0) {
                    ps.executeBatch();
                }

                //ps.executeUpdate();
               // conn.commit();
            }

            lineReader.close();

            // execute the remaining queries
            ps.executeBatch();

            this.conn.commit();
            this.conn.close();
        } catch (IOException ex) {
            System.err.println(ex);
        }catch (SQLException e) {
            e.printStackTrace();
            try{
                conn.rollback();
            }catch (SQLException e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }finally{
            try{
                if(ps != null){
                    ps.close();
                }
            }catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
    }


    /**
     * The function create table MEDIAITEMS
     */
    @SuppressWarnings("unused")
    private  void createTable(){
        if(this.conn==null){
            //connect();
        }
        PreparedStatement ps = null;
        String query = "CREATE TABLE MediaItems"+
                " ("+
                " MID BIGINT NOT NULL,"+
                " TITLE varchar(200),"+
                " PROD_YEAR BIGINT, " +
                " TITLE_LENGTH BIGINT, " +
                " MID1 BIGINT NOT NULL,"+
                " MID2 BIGINT NOT NULL,"+
                " CONSTRAINT MEDIAITEMS_PK PRIMARY KEY"+
                " ("+
                " MID"+
                " )"+
                " )"; //query
        try{
            ps = conn.prepareStatement(query); //compiling query in the DB
            ps.executeUpdate();
            conn.commit();
        }catch (SQLException e) {
            try{
                conn.rollback();
            }catch (SQLException e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }finally{
            try{
                if(ps != null){
                    ps.close();
                }
            }catch (SQLException e3) {
                e3.printStackTrace();
            }
        }
    }


    public static void main(String[] args) {
        Assignment1 t= new Assignment1("mayasos","mayasos","rb2SWVYd"); //constructor
        //t.connect();
        t.fileToDataBase("films.csv");
		//t.createTable();
//		t.insertData(1, "Pulp Fiction", 1994);
//		t.insertData(2, "The Silence of the Lambs", 1991);
//		t.printData();
        t.disconnect();
    }
}