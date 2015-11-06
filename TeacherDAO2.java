package db.daos.map;
import java.util.ArrayList;
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

	private void createTable() {
		String sql = String
				.format("CREATE TABLE %s.teacher (id bigint primary key, name text, title text,courses list<bigint>, office Map<text, bigint>)",
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
	private void updateById(Map<String, Long> office, Long id) {
		String sql = String
				.format("UPDATE %s.teacher SET office = ? WHERE id = ?",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(office, id);
			conn.execute(bs);
		} finally {
			conn.close();
		}
	}
	//TODO
	private void addToMap(Map<String, Long> office, Long added, Long id) {
		
		String sql = String
				.format("UPDATE %s.teacher SET office[' ? '] = ' " + added + " '  WHERE id = ?",
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
	public static void main(String[] args) {
		TeacherDAO teacherDao = TeacherDAO.getInstance();
		 teacherDao.dropTable();
		 teacherDao.createTable();

		 Teacher obj1 = new Teacher();
		 obj1.setName("Ravi");
		 obj1.setId(1l);
		 obj1.setTitle("Teacher");
		 List<Long> course = new ArrayList<Long>();
		 course.add(15213l);
		 course.add(10000l);
		 obj1.setCourses(course);	 
		 Map<String, Long> office = new HashMap<String, Long>();
		 office.put("Pitts", 1l);
		 office.put("Pitts", 2l);
		 
		 obj1.setOffice(office);
		 teacherDao.insert(obj1);
		 
		 office.put("Seattle", 3l);
		 //teacherDao.updateById(office, 1l);
		 teacherDao.addToMap(obj1.getOffice(), 4l, 1l);
	}
}
