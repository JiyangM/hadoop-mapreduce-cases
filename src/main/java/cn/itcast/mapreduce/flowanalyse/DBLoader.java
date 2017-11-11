package cn.itcast.mapreduce.flowanalyse;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

/**
 * mysql数据库需要开放远程登录访问，做法：
 * 先到mysql服务器上登录mysql，然后在mysql提示符下输入
 *     mysql>grant all privileges on *.* to 'root'@'%' identified by 'root' with grant option;
 *     mysql>flush privileges;
 * 
 * 
 * @author
 *
 */

public class DBLoader {

	public static void loadDB(HashMap<String, String> ruleMap) throws Exception {

		Connection conn = null;
		Statement st = null;
		ResultSet res = null;
		
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://mini1:3306/urldb", "root", "123321");
			st = conn.createStatement();
			res = st.executeQuery("select url,info from url_rule");
			while (res.next()) {
				ruleMap.put(res.getString(1), res.getString(2));
			}

		} finally {
			try{
				if(res!=null){
					res.close();
				}
				if(st!=null){
					st.close();
				}
				if(conn!=null){
					conn.close();
				}

			}catch(Exception e){
				e.printStackTrace();
			}
		}

	}

}
