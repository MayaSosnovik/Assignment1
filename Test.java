import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Test {
    private Connection conn =null;

    /**
     * Constructor
     */
    public Test() {
        super();
        connect();
    }
    /**
     * The function makes the connection to the DB
     */


    private void connect(){
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            String connectionUrl = "jdbc:sqlserver://132.72.64.124:1433;databaseName=amitpart;user=amitpart;password=G7DkEb2Y;encrypt=false;";
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


    /**
     * The function create table MEDIAITEMS
     */
    @SuppressWarnings("unused")
    private  void createTable(){
        if(this.conn==null){
            connect();
        }
        PreparedStatement ps = null;
        String query = "CREATE TABLE MediaItems"+
                " ("+
                " MID NUMBER(11, 0) NOT NULL,"+
                " TITLE nvarchar(200),"+
                " PROD_YEAR NUMBER(4), " +
                " TITLE_LENGTH (NUMBER(6)), " +
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
        Test t= new Test(); //constructor
        t.connect();
		t.createTable();
//		t.insertData(1, "Pulp Fiction", 1994);
//		t.insertData(2, "The Silence of the Lambs", 1991);
//		t.printData();
        t.disconnect();
    }
}