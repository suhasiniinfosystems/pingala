package collection.db.test;

import java.util.ArrayList;
import java.util.List;

import collection.db.Copy;
import collection.db.PKOne;
import collection.db.TransactionTableNoKey;
import collection.db.TransactionTableOneKey;
import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

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

		PKOne<String, Person> pkOne = new PKOneImpl();
		Copy<Person> personCopier = new PersonCopier();
		
		TransactionTableNoKey<Person> transactionalTable = new TransactionTableNoKey<Person>(personCopier, personList);

		SnapshotCursor<Person> iteratorForRead = transactionalTable.createSnapshotCursor();
		while ( iteratorForRead.next() ) {
			Person person = iteratorForRead.get();
			System.out.println(person);
		}

		System.out.println("------------- init done --------------");
		
		ReadWriteCursor<Person> iteratorForUpdate = transactionalTable.createReadWriteCursor();
		while ( iteratorForUpdate.next() ) {
			Person person = iteratorForUpdate.get();
			person.setAge(person.getAge() - 10);
			iteratorForUpdate.update(person);
		}
		iteratorForUpdate.commit();
		
		System.out.println("------------- update done --------------");
		iteratorForRead = transactionalTable.createSnapshotCursor();
		while ( iteratorForRead.next() ) {
			Person person = iteratorForRead.get();
			System.out.println(person);
		}
		System.out.println("------------- dump all --------------");
		transactionalTable.dump();
	}

}
