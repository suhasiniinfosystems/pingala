package collection.db.test;

import collection.db.PKOne;

public class PKOneImpl implements PKOne<String, Person> {

	public String getPKOne(Person person) {
		return person.getName();
	}
	
}
