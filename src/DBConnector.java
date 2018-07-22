import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DBConnector {

	private String url;
	private String user;
	private String password;
	
	Connection conn = null;
	PreparedStatement pstmt = null;
	ResultSet rs = null;
	ResultSetMetaData rsMeta = null;
	
	public DBConnector(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}
	
	public void connect(String driver) throws ClassNotFoundException, SQLException {
		Class.forName(driver);
		conn = DriverManager.getConnection(url, user, password);
	}
	
	public void close() throws SQLException {
		conn.close();
	}
	
	public void setAutoCommit(boolean b) throws SQLException {
		conn.setAutoCommit(b);
	}
	
	public void commit() throws SQLException {
		conn.commit();
	}
	
	public void rollback() throws SQLException {
		conn.rollback();
	}
	
	private void parameterBind(String query, Map<String, Object> parameters) throws SQLException {
		String parameterRegex = "\\{([_A-Za-z])+\\}";
		Pattern p = Pattern.compile(parameterRegex);
		Matcher m = p.matcher(query);
		
		List<String> bindParameterList = new ArrayList<String>();
		while (m.find()) {
			bindParameterList.add(m.group().replaceAll("\\{|\\}", ""));
		}
		
		query = query.replaceAll("\\{([_A-Za-z])+\\}", "?");
		
		pstmt = conn.prepareStatement(query);
		if (parameters != null) {
			for (int i = 0; i < bindParameterList.size(); i++) {
				String name = bindParameterList.get(i);
				if (parameters.get(name) instanceof String) {
					pstmt.setString(i + 1, (String) parameters.get(name));
				} else if (parameters.get(name) instanceof Integer) {
					pstmt.setInt(i + 1, (Integer) parameters.get(name));
				} else if (parameters.get(name) instanceof byte[]) {
					byte[] bArray = (byte[]) parameters.get(name);
					pstmt.setBinaryStream(i + 1, new ByteArrayInputStream(bArray), bArray.length);
				} else if (parameters.get(name) == null) {
					pstmt.setNull(i + 1, Types.NULL);
				} else {
					System.out.println(" Type error >> name : " + name);
					throw new RuntimeException("bind exception!");
				}
			}
		}
	}
	
	private Map<String, Object> rsToMap(ResultSet rs) throws SQLException {
		Map<String, Object> map = new HashMap<String, Object>();
		try {
			rsMeta =  rs.getMetaData();
			int columnCount = rsMeta.getColumnCount();
			for (int i = 1; i <= columnCount; i ++) {
				switch (rsMeta.getColumnType(i)) {
				case Types.NUMERIC :
				case Types.INTEGER :
					if ((Integer) rs.getInt(i) != null) {
						map.put(rsMeta.getColumnName(i), rs.getInt(i));
					} else {
						map.put(rsMeta.getColumnName(i), null);
					}
					break;
				case Types.CHAR :
				case Types.VARCHAR :
				case Types.NVARCHAR :
				case Types.CLOB :
				case Types.TIMESTAMP :
					if (rs.getString(i) != null) {
						map.put(rsMeta.getColumnName(i), rs.getString(i));
					} else {
						map.put(rsMeta.getColumnName(i), null);
					}
					break;
				case Types.OTHER :
					System.out.println(" other type >> " + rsMeta.getColumnType(i));
					break;
				default :
					System.out.println(" not match >> name : " + rsMeta.getColumnName(i) + " / type : " + rsMeta.getColumnType(i));
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return map;
	}
	
	private List<Map<String, Object>> rsToList(ResultSet rs) throws SQLException {
		List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
		try {
			do {
				mapList.add(rsToMap(rs));
			} while (rs.next());
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		}
		return mapList;
	}
	
	
	/* ** CRUD ** */
	public Map<String, Object> select(String query, Map<String, Object> parameters) throws SQLException {
		parameterBind(query, parameters);
		rs = pstmt.executeQuery();
		Map<String, Object> resultValue = rs.next() ? rsToMap(rs) : new HashMap<String, Object>();
		
		if (rs != null) rs.close();
		if (pstmt != null) pstmt.close();
		
		return resultValue;
	}
	
	public List<Map<String, Object>> selectList(String query, Map<String, Object> parameters) throws SQLException {
		parameterBind(query, parameters);
		rs = pstmt.executeQuery();
		List<Map<String, Object>> resultValue = rs.next() ? rsToList(rs) : new ArrayList<Map<String, Object>>();
		
		if (rs != null) rs.close();
		if (pstmt != null) pstmt.close();
		
		return resultValue;
	}
	
	public int insert(String query, Map<String, Object> parameters) throws SQLException {
		parameterBind(query, parameters);
		int count = pstmt.executeUpdate();
		
		if (rs != null) rs.close();
		if (pstmt != null) pstmt.close();
		
		return count;
	}
	
	public int update(String query, Map<String, Object> parameters) throws SQLException {
		return insert(query, parameters);
	}
	
	public int delete(String query, Map<String, Object> parameters) throws SQLException {
		return insert(query, parameters);
	}
}
