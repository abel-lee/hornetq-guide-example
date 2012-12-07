package org.asdtiang.hornetq.hornetq_example;

import java.net.UnknownHostException;

import org.junit.Test;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class TestMongo {

	@Test
	public void test() {
		Mongo mg = null;
		try {
			mg = new Mongo();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// 查询所有的Database
		for (String name : mg.getDatabaseNames()) {
			System.out.println("dbName: " + name);
			DB db = mg.getDB(name);
			// 查询所有的聚集集合
			for (String collName : db.getCollectionNames()) {
				System.out.println("collectionName: " + collName);
				DBCollection users = db.getCollection(collName);
				// 查询所有的数据
				DBCursor cur = users.find();
				while (cur.hasNext()) {
					System.out.println(cur.next());
				}
				System.out.println(cur.count());

				System.out.println(cur.getCursorId());

				System.out.println(JSON.serialize(cur));
			}
			
		}
		

	}

}
