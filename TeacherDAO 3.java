package db.daos.map;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import db.models.Teacher;
//http://docs.datastax.com/en/cql/3.0/cql/cql_using/use_map_t.html
public class TeacherDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static TeacherDAO instance = null;

	public static TeacherDAO getInstance() {
		if (null == instance) {
			instance = new TeacherDAO("demo");
		}
		return instance;
	}

	public static TeacherDAO getNewInstance(String keyspaceName) {
		return new TeacherDAO(keyspaceName);
	}
	
	private final String keyspaceName;

	private TeacherDAO(String keyspaceName) {
		this.keyspaceName = keyspaceName;
	}
	
	private void setInfor(Teacher obj, Row row) {
		
		obj.setId(row.getLong("id"));
	    obj.setName(row.getString("name"));
	    obj.setTitle(row.getString("title"));
	    obj.setCourses(row.getList("courses", Long.class));
	    obj.setOffice(row.getMap("office", String.class, String.class));
	}
	
	private void createTable() {
		String sql = String
				.format("CREATE TABLE %s.teacher (id bigint primary key, name text, title text,courses list<bigint>, office Map<text, text>)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}

	private void dropTable() {
		String sql = String.format("DROP TABLE %s.teacher", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			conn.execute(sql);
		} finally {
			conn.close();
		}
	}
	private void insert(Teacher obj) {
		String sql = String
				.format("INSERT INTO %s.teacher(id,name,title,courses,office) VALUES (?,?,?,?,?)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(obj.getId(), obj.getName(), obj.getTitle(), obj.getCourses(), obj.getOffice());
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	private void updateById(Map<String, String> office1, Long id) {
		/*String sql = String
				.format("UPDATE %s.teacher SET office['" + office.keySet() + "'] = '" + office.values() + "' WHERE id = ?", keyspaceName);*/
		/*UPDATE users SET todo['2012-10-2 12:00'] = 'throw my precious into mount doom'
		  WHERE user_id = 'frodo';*/
		String sql = String
				.format("UPDATE %s.teacher SET office = ? WHERE id = ?", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(office1, id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	//TODO
	private void updateByKey(Map<String, String> office, Long id) {
		String officeLoc = office.keySet().toString();
		String str1 = officeLoc.substring(1, officeLoc.length()-2);
		String officeNum = office.values().toString();
		String str2 = officeNum.substring(1, officeNum.length()-1);
		String sql = String
				.format("UPDATE %s.teacher SET office['" + str1+ "'] = '"+ str2+"' WHERE id = ?",
						keyspaceName);
		
		/*UPDATE users SET todo['2012-10-2 12:00'] = 'throw my precious into mount doom'
				  WHERE user_id = 'frodo';*/
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(id);
			conn.execute(bs);
		} finally {
			conn.close();
		}	
	}
	//Same as set operation
	private List<Teacher> selectById(Long id) {
		if (null == id) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.teacher WHERE id = ?", keyspaceName);
		System.out.println(sql);
		
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		
		try {
			PreparedStatement ps = conn.prepare(sql);
			BoundStatement bs = ps.bind(id);
			ResultSet rs = conn.execute(bs);
			
			for (Row row : rs) {
				Teacher obj = new Teacher();
				setInfor(obj, row);
				result.add(obj);			
			}
		} finally {
			conn.close();
		}
		return result;
	}
	private List<Teacher> selectAll() {
		String sql = String.format("SELECT * FROM %s.teacher", keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		List<Teacher> result = new ArrayList<Teacher>();
		try {
			ResultSet rs = conn.execute(sql);
			for (Row row : rs) {    
				Teacher obj = new Teacher();
				obj.setId(row.getLong("id"));
				obj.setName(row.getString("name"));
				obj.setTitle(row.getString("title"));
				obj.setCourses(row.getList("courses", Long.class));
				obj.setOffice(row.getMap("office", String.class, String.class));
				result.add(obj);
			}
		} finally {
			conn.close();
		}
		return result;
	}
	//DELETE todo['2012-9-24'] FROM users WHERE user_id = 'frodo';
	private void deleteFromMap(String officeLoc, Long id) {
		String sql = String
				.format("DELETE office['"+ officeLoc +"'] FROM %s.teacher WHERE id = ?",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(id);
			conn.execute(bs);
		} finally {
			conn.close();
		}	
		
	}
	public static void main(String[] args) {
		TeacherDAO teacherDao = TeacherDAO.getInstance();
		 teacherDao.dropTable();
		 teacherDao.createTable();

		 Teacher obj1 = new Teacher();
		 obj1.setName("Ravi");
		 obj1.setId(1l);
		 obj1.setTitle("Teacher");
		 	 
		 Map<String, String> office1 = new HashMap<String, String>();
		 office1.put("Pitts", "GHC");
		 office1.put("SanJose", "A");
		 office1.put("Seattle", "B");
		 
		 obj1.setOffice(office1);
		 teacherDao.insert(obj1);
		 
		/* Map<String, String> office2 = new HashMap<String, String>();
		 office2.put("Pitts", "PHDS");*/
		 //teacherDao.updateByKey(office2, 1l);
		 
		 Teacher obj2 = new Teacher();
		 obj2.setName("John");
		 obj2.setId(2l);
		 obj2.setTitle("Teacher");
		 	 
		 Map<String, String> office3 = new HashMap<String, String>();
		 office3.put("Pitts", "GHC2");
		 office3.put("SanJose", "A2");
		 office3.put("Seattle", "B2");
		 
		 obj2.setOffice(office3);
		 teacherDao.insert(obj2);
		 teacherDao.deleteFromMap("Pitts", 2l);
		 
		 //teacherDao.updateById(office1, 1l);
		 //teacherDao.selectAll();
		 //teacherDao.selectById(2l);
	}
}
