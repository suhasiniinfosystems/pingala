package collection.db.test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import collection.db.Copy;
import collection.db.DuplicateKeysExistsException;
import collection.db.PrimaryKey;
import collection.db.TransactionTableTwoKeys;
import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

public class Main_List_TwoKey {

	/*
	 * Test two writes against the same record...
	 * 
	 */
	public static void main(String[] args) throws Exception {
		
		List<Person> personList = new ArrayList<Person>();
		Person person = new Person();
		person.setName("Sanjay");
		person.setAge(50);
		personList.add(person);

		Person person2 = new Person();
		person2.setName("Sanju");
		person2.setAge(51);
		personList.add(person2);

		
		PrimaryKey<String, Person> personKeyName = new PersonKeyName();
		PrimaryKey<Integer, Person> personKeyAge = new PersonKeyAge();

		Copy<Person> personCopier = new PersonCopier();
		
		TransactionTableTwoKeys<Person, String, Integer> transactionTable = new TransactionTableTwoKeys<Person, String, Integer>(personCopier, personList, personKeyName, personKeyAge);
		
		Thread thread = new Thread(new Runnable1(transactionTable), "Thread-1");
		
		thread.start();

		thread.join();
		
		System.out.println("------------- dump all --------------");
		transactionTable.dump();

		System.out.println("------------- list all --------------");
		SnapshotCursor<Person> iteratorForRead = transactionTable.createSnapshotCursor();
		while ( iteratorForRead.next() ) {
			Person person0 = iteratorForRead.get();
			System.out.println(person0);
		}
		
		
		System.out.println("Done.................");
	}
	
	static class Runnable1 implements Runnable {
		
		TransactionTableTwoKeys<Person, String, Integer> transactionTable;		
		Runnable1(TransactionTableTwoKeys<Person, String, Integer> transactionTable) {
			this.transactionTable = transactionTable;
		}
		
		public void run() {

			ReadWriteCursor<Person> iteratorForUpdate = transactionTable.createReadWriteCursor();
			
			Person person = new Person();
			person.setName("Sanjay1");
			person.setAge(52);
			try {
				iteratorForUpdate.insert(person);
				iteratorForUpdate.commit();
				System.out.println(Thread.currentThread().getName() + " commited");
			} catch (DuplicateKeysExistsException e) {
				e.printStackTrace();
			}
			
		}
		
	}

}
