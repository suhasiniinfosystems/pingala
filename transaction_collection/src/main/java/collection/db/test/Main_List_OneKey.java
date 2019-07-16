package collection.db.test;

import java.util.ArrayList;
import java.util.List;

import collection.db.Copy;
import collection.db.DuplicateKeysExistsException;
import collection.db.PrimaryKey;
import collection.db.TransactionTableOneKey;
import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

public class Main_List_OneKey {

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

		Copy<Person> personCopier = new PersonCopier();
		
		TransactionTableOneKey<Person, String> transactionTable = new TransactionTableOneKey<Person, String>(personCopier, personList, personKeyName);
		
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
		
		TransactionTableOneKey<Person, String> transactionTable;		
		Runnable1(TransactionTableOneKey<Person, String> transactionTable) {
			this.transactionTable = transactionTable;
		}
		
		public void run() {

			ReadWriteCursor<Person> iteratorForUpdate = transactionTable.createReadWriteCursor();
			
			Person person = new Person();
			person.setName("Sanjay");
			person.setAge(53);
			try {
				iteratorForUpdate.insert(person);
			} catch (DuplicateKeysExistsException e) {
				iteratorForUpdate.rollback();
				e.printStackTrace();
			}

			iteratorForUpdate = transactionTable.createReadWriteCursor();
			try {
				person.setName("Sanjay2");
				iteratorForUpdate.insert(person);
			} catch (DuplicateKeysExistsException e) {
				e.printStackTrace();
			}
			iteratorForUpdate.commit();

			System.out.println(Thread.currentThread().getName() + " commited");
			
		}
		
	}

}
