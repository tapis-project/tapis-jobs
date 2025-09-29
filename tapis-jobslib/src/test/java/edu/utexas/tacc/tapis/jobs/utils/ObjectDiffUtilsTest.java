package edu.utexas.tacc.tapis.jobs.utils;

import java.util.List;

import org.testng.annotations.Test;

import edu.utexas.tacc.tapis.jobs.utils.ObjectDiffUtils.ListDiff;
import edu.utexas.tacc.tapis.jobs.utils.ObjectDiffUtils.ObjectDiff;

@Test(groups={"unit"})
public class ObjectDiffUtilsTest {

    public class Example {
        public String name;
        public int age;
        public List<String> tags;

        Example(String name, int age, List<String> tags) {
            this.name = name;
            this.age = age;
            this.tags = tags;
        }
    }

    public class Example2 {
        public String name;
        public int age;
        public String gender;
        public List<String> tags;

        Example2(String name, int age, String gender, List<String> tags) {
            this.name = name;
            this.age = age;
            this.gender = gender;
            this.tags = tags;
        }
    }

    @Test
    public void testObjectDiffUtils() {
        try {

            Example oldObj = new Example("Alice", 30, List.of("tag1", "tag2"));
            Example2 newObj = new Example2("Alice", 31, "Female", List.of("tag2", "tag3"));

            ObjectDiff diff = ObjectDiffUtils.computeObjectDiff(oldObj, newObj);
            System.out.println(diff.toJsonString());

            ListDiff<String> listDiff = ObjectDiffUtils.computeListDiff(oldObj.tags, newObj.tags);
            System.out.println(listDiff.toJsonString());

            List<String> oldList = List.of("apple", "banana", "apple", "cherry");
            List<String> newList = List.of("apple", "banana", "banana", "date", "apple");

            ListDiff<String> freqDiff = ObjectDiffUtils.computeListDiff(oldList, newList);
            System.out.println(freqDiff.toJsonString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
