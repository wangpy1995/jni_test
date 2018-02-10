package test.jni;

import wpy.test.JniObject;
import wpy.test.Student;

public class TestJniObject {

    public static void main(String[] args) {
        System.load("/home/wpy/CLionProjects/jni_test/cmake-build-debug/libJniObject.so");
        JniObject obj = new JniObject();
        Student student = new Student();
        student.id = 2;
        boolean c = obj.changeStudent(student);
        System.out.println("change: " + c + " id: " + student.id);
        System.out.println();
        Student s2 = obj.createStudent();
        printStudent(s2);

        System.out.println(obj.newLongValue());
    }

    private static void printStudent(Student student) {
        System.out.println("id: " + student.id);
        System.out.println("name: " + student.name);
        System.out.println("gender: " + student.gender);
        System.out.println("age: " + student.age);
        System.out.println("comment: " + student.comment);
    }
}
