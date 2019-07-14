package collection.db.test;

import java.util.ArrayList;
import java.util.List;

import collection.db.DbList;
import collection.db.DbList.ReadCursor;
import collection.db.DbList.UpdatableCursor;

public class Main_List2 {

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

		DbList<Person> dbList = new DbList<Person>(personList);

		
		ReadCursor<Person> iteratorForRead = dbList.createReadCursor();
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
		
		DbList<Person> dbList;		
		Runnable1(DbList<Person> dbList) {
			this.dbList = dbList;
		}
		
		public void run() {

			UpdatableCursor<Person> iteratorForUpdate = dbList.createUpdatableCursor();
			if ( iteratorForUpdate.next() ) {
				System.out.println(Thread.currentThread().getName() + " Is processing");
				Person person = iteratorForUpdate.get();
				try {
					//Thread.sleep(2000);
				} catch (Exception e) {}
				person.setAge(person.getAge() - 10);
				iteratorForUpdate.update(person);
				try {
					//Thread.sleep(2000);
				} catch (Exception e) {}
				iteratorForUpdate.commit();
				System.out.println(Thread.currentThread().getName() + " commited");
			}

		}
		
	}

	static class Runnable2 implements Runnable {
		
		DbList<Person> dbList;
		Runnable2(DbList<Person> dbList) {
			this.dbList = dbList;
		}
		
		public void run() {

			UpdatableCursor<Person> iteratorForUpdate = dbList.createUpdatableCursor();
			if ( iteratorForUpdate.next() ) {
				System.out.println(Thread.currentThread().getName() + " Is processing");
				Person person = iteratorForUpdate.get();
				try {
					//Thread.sleep(2000);
				} catch (Exception e) {}
				person.setAge(person.getAge() - 10);
				iteratorForUpdate.update(person);
				try {
					//Thread.sleep(2000);
				} catch (Exception e) {}
				iteratorForUpdate.commit();
				System.out.println(Thread.currentThread().getName() + " commited");
			}

		}
		
	}

}
