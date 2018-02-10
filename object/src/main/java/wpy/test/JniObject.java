package wpy.test;

public class JniObject {

    public native Student createStudent();

    public native boolean changeStudent(Student student);

    public native long newLongValue();
}
