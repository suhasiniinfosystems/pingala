package collection.db.test;

import collection.db.PrimaryKey;

public class PersonKeyAge implements PrimaryKey<Integer, Person> {

	public String getKeyName() {
		return "age";
	}

	public Integer getKeyValue(Person person) {
		return person.getAge();
	}
	
}
