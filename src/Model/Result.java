package Model;

public class Result {
    public int rowAffected;

    public Result(int rowAffected) {
        this.rowAffected = rowAffected;
    }

    public void Display() {
        System.out.println("Query Successful");
        System.out.println(String.format("%d rows", this.rowAffected));
        System.out.println();
    }
}
