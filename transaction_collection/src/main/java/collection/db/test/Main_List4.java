package collection.db.test;

import java.util.ArrayList;
import java.util.List;

import collection.db.TransactionalTable;
import collection.db.TransactionalTable.ReadWriteCursor;
import collection.db.TransactionalTable.SnapshotCursor;

public class Main_List4 {

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

		Person person1 = new Person();
		person1.setName("Amar");
		person1.setAge(44);
		//personList.add(person1);

		Person person2 = new Person();
		person2.setName("Ashwini");
		person2.setAge(33);
		//personList.add(person2);

		TransactionalTable<Person> transactionalTable = new TransactionalTable<Person>(personList);

		
		SnapshotCursor<Person> iteratorForRead = transactionalTable.createSnapshotCursor();
		while ( iteratorForRead.next() ) {
			Person person = iteratorForRead.get();
			System.out.println(person);
		}
		
		Thread thread = new Thread(new Runnable1(transactionalTable), "Thread-1");
		Thread thread2 = new Thread(new Runnable2(transactionalTable), "Thread-2");
		
		thread.start();
		Thread.sleep(1000);
		thread2.start();

		thread.join();
		thread2.join();
		
		System.out.println("------------- dump all --------------");
		transactionalTable.dump();

		
		System.out.println("Done.................");
	}
	
	static class Runnable1 implements Runnable {
		
		TransactionalTable<Person> transactionalTable;		
		Runnable1(TransactionalTable<Person> transactionalTable) {
			this.transactionalTable = transactionalTable;
		}
		
		public void run() {
			ReadWriteCursor<Person> iteratorForUpdate = transactionalTable.createReadWriteCursor();
			if ( iteratorForUpdate.next() ) {
				System.out.println(Thread.currentThread().getName() + " Is processing");
				Person person = iteratorForUpdate.get();
				try {
					Thread.sleep(2000);
				} catch (Exception e) {}
				person.setAge(person.getAge() - 10);
				iteratorForUpdate.update(person);
				try {
					Thread.sleep(2000);
				} catch (Exception e) {}
				iteratorForUpdate.rollback();
				System.out.println(Thread.currentThread().getName() + " commited");
			}

		}
		
	}

	static class Runnable2 implements Runnable {
		
		TransactionalTable<Person> transactionalTable;
		Runnable2(TransactionalTable<Person> transactionalTable) {
			this.transactionalTable = transactionalTable;
		}
		
		public void run() {
			SnapshotCursor<Person> iterator = transactionalTable.createSnapshotCursor();
			if ( iterator.next() ) {
				System.out.println(Thread.currentThread().getName() + " Is processing");
				Person person = iterator.get();
				person.setAge(person.getAge() - 10);
				System.out.println(Thread.currentThread().getName() + " done");
			}

		}
		
	}

}
