package collection.db.test;

import java.util.ArrayList;
import java.util.List;

import collection.db.DbList;
import collection.db.DbList.ReadCursor;
import collection.db.DbList.UpdatableCursor;

public class Main_List {

	public static void main(String[] args) throws Exception {
		
		List<Person> personList = new ArrayList<Person>();
		Person person0 = new Person();
		person0.setName("Sanjay");
		person0.setAge(50);
		personList.add(person0);

		Person person1 = new Person();
		person1.setName("Amar");
		person1.setAge(44);
		personList.add(person1);

		Person person2 = new Person();
		person2.setName("Ashwini");
		person2.setAge(33);
		personList.add(person2);

		DbList<Person> dbList = new DbList<Person>(personList);

		ReadCursor<Person> iteratorForRead = dbList.createReadCursor();
		while ( iteratorForRead.next() ) {
			Person person = iteratorForRead.get();
			System.out.println(person);
		}

		System.out.println("------------- init done --------------");
		
		UpdatableCursor<Person> iteratorForUpdate = dbList.createUpdatableCursor();
		while ( iteratorForUpdate.next() ) {
			Person person = iteratorForUpdate.get();
			person.setAge(person.getAge() - 10);
			iteratorForUpdate.update(person);
		}
		iteratorForUpdate.commit();
		
		System.out.println("------------- update done --------------");
		iteratorForRead = dbList.createReadCursor();
		while ( iteratorForRead.next() ) {
			Person person = iteratorForRead.get();
			System.out.println(person);
		}
		System.out.println("------------- dump all --------------");
		dbList.dump();
	}

}
