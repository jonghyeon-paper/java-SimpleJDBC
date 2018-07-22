import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TestConnection {

	public static void main(String[] args) {
		
		DBConnector conn = new DBConnector("jdbc:mysql://localhost:3306/sample", "sample", "sample");
		try {
			conn.connect("com.mysql.jdbc.Driver");
			
			String query = "SELECT {A} AS A, {B} AS B FROM DUAL";
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("A", "A123");
			params.put("B", "B123");
			
			Map <String, Object> result = conn.select(query, params);
			
			System.out.println(result);
		} catch (ClassNotFoundException | SQLException e) {
			e.printStackTrace();
		}

	}

}
