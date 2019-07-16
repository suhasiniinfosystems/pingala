package collection.db.test;

import collection.db.PKTwo;

public class PKTwoImpl implements PKTwo<Integer, Person> {

	public Integer getPKTwo(Person person) {
		return person.getAge();
	}
	
}
