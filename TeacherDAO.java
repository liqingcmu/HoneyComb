package db.daos;

import java.util.ArrayList;
import java.util.List;

import utils.connectionPool.CassandraConnection;
import utils.connectionPool.ConnectionPool;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import db.models.Teacher;

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
				.format("CREATE TABLE %s.teacher (id bigint PRIMARY KEY,name text, title text,courses list<bigint>)",
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
				.format("INSERT INTO %s.teacher(id,name,title,courses) VALUES (?,?,?,?)",
						keyspaceName);
		System.out.println(sql);
		CassandraConnection conn = pool.getConnection();
		try {
			PreparedStatement ps = conn.prepare(sql);
			System.out.println(ps);
			BoundStatement bs = ps.bind(obj.getId(), obj.getName(), obj.getTitle(), obj.getCourses());
			conn.execute(bs);
		} finally {
			conn.close();
		}
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
				result.add(obj);
			}
		} finally {
			conn.close();
		}
		return result;
	}
	
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
	private void setInfor(Teacher obj, Row row) {
		
		obj.setId(row.getLong("id"));
	    obj.setName(row.getString("name"));
	    obj.setTitle(row.getString("title"));
	    obj.setCourses(row.getList("courses", Long.class));

	}

	public static void main(String[] args) {
		TeacherDAO teacherDao = TeacherDAO.getInstance();
		teacherDao.dropTable();
		teacherDao.createTable();

		Teacher obj = new Teacher();
		obj.setId((long)1);
		obj.setName("Ravi");
		obj.setTitle("Teacher");
		List<Long> course = new ArrayList<Long>();
		course.add(1234l);
		course.add(5678l);
		obj.setCourses(course);
		teacherDao.insert(obj);
		
		Teacher obj1 = new Teacher();
		obj1.setId((long)2);
		obj1.setName("Ralf");
		obj1.setTitle("Teacher");
		List<Long> course1 = new ArrayList<Long>();
		course1.add(2345l);
		course1.add(6789l);
		obj1.setCourses(course1);
		teacherDao.insert(obj1);

		
		Teacher obj2 = new Teacher();
		obj2.setId((long)3);
		obj2.setName("Xingyu");
		obj2.setTitle("Teacher");
		List<Long> course2 = new ArrayList<Long>();
		course2.add(3456l);
		course2.add(7890l);
		obj2.setCourses(course2);
		teacherDao.insert(obj2);

		
		Teacher obj3 = new Teacher();
		obj3.setId((long)4);
		obj3.setName("Jianhe");
		obj3.setTitle("Teacher");
		List<Long> course3 = new ArrayList<Long>();
		course3.add(5786l);
		course3.add(1253l);
		obj3.setCourses(course3);
		teacherDao.insert(obj3);

		

		List<Teacher> selectAll = teacherDao.selectAll();
		for (Teacher t : selectAll) {
			System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getTitle());
		}
		
		List<Teacher> selectbyid = teacherDao.selectById((long)1);
		for (Teacher t : selectbyid) {
			System.out.println(t.getId() + "\t|\t" + t.getName() + "\t|\t" + t.getTitle() + "\t|\t" + t.getCourses() + "\t|\t");
		}

		ConnectionPool.getInstance().close();
	}
}
