package collection.db.test;

import java.util.ArrayList;
import java.util.List;

import collection.db.Copy;
import collection.db.TransactionTableNoKey;
import collection.db.TransactionalTable;
import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

public class Main_List2 {

	/*
	 * Test two writes against the same record...
	 * 
	 */
	public static void main(String[] args) throws Exception {
		
		List<Person> personList = new ArrayList<Person>();
		Person person0 = new Person();
		person0.setName("Sanjay");
		person0.setAge(50);
		personList.add(person0);

		Copy<Person> personCopier = new PersonCopier();
		TransactionTableNoKey<Person> dbList = new TransactionTableNoKey<Person>(personCopier, personList);
		
		SnapshotCursor<Person> iteratorForRead = dbList.createSnapshotCursor();
		while ( iteratorForRead.next() ) {
			Person person = iteratorForRead.get();
			System.out.println(person);
		}
		
		Thread thread = new Thread(new Runnable1(dbList), "Thread-1");
		Thread thread2 = new Thread(new Runnable2(dbList), "Thread-2");
		
		thread.start();
		thread2.start();

		thread.join();
		thread2.join();
		
		System.out.println("------------- dump all --------------");
		dbList.dump();

		
		System.out.println("Done.................");
	}
	
	static class Runnable1 implements Runnable {
		
		TransactionTableNoKey<Person> dbList;		
		Runnable1(TransactionTableNoKey<Person> dbList) {
			this.dbList = dbList;
		}
		
		public void run() {

			ReadWriteCursor<Person> iteratorForUpdate = dbList.createReadWriteCursor();
			if ( iteratorForUpdate.next() ) {
				System.out.println(Thread.currentThread().getName() + " Is processing");
				Person person = iteratorForUpdate.get();
				try {
					Thread.sleep(2000);
					person.setAge(person.getAge() - 8);
					iteratorForUpdate.update(person);
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(2000);
				} catch (Exception e) {}
				iteratorForUpdate.commit();
				System.out.println(Thread.currentThread().getName() + " commited");
			}

		}
		
	}

	static class Runnable2 implements Runnable {
		
		TransactionTableNoKey<Person> dbList;
		Runnable2(TransactionTableNoKey<Person> dbList) {
			this.dbList = dbList;
		}
		
		public void run() {

			ReadWriteCursor<Person> iteratorForUpdate = dbList.createReadWriteCursor();
			if ( iteratorForUpdate.next() ) {
				System.out.println(Thread.currentThread().getName() + " Is processing");
				Person person = iteratorForUpdate.get();
				try {
					Thread.sleep(2000);
					person.setAge(person.getAge() - 9);
					iteratorForUpdate.update(person);
				} catch (Exception e) {
					e.printStackTrace();
				}
				try {
					Thread.sleep(2000);
				} catch (Exception e) {}
				iteratorForUpdate.commit();
				System.out.println(Thread.currentThread().getName() + " commited");
			}

		}
		
	}

}
