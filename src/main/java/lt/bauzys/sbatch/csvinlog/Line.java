package lt.bauzys.sbatch.csvinlog;

import java.time.LocalDate;

public class Line {
    private String name;
    private LocalDate dob;
    private Long age;

    public Line() {
    }

    public Line(String name, LocalDate dob) {
        this.name = name;
        this.dob = dob;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public LocalDate getDob() {
        return dob;
    }

    public void setDob(String dobString) {

    }

    public void setDob(LocalDate dob) {
        this.dob = dob;
    }

    public Long getAge() {
        return age;
    }

    public void setAge(Long age) {
        this.age = age;
    }
}
