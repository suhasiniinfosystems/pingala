package collection.db.test;

import collection.db.Copy;

public class PersonCopier implements Copy<Person> {

	public Person copy(Person person) {
		Person personNew = new Person();
		personNew.setName(person.getName());
		personNew.setAge(person.getAge());
		return personNew;
	}
	
	
}
