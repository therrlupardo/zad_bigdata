import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class VisitorTuple implements WritableComparable<VisitorTuple> {

    private String firstName = "";
    private String midName = "";
    private String lastName = "";

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        if(firstName != null) this.firstName = firstName;
        else this.firstName = "";
    }

    public String getMidName() {
        return midName;
    }

    public void setMidName(String midName) {
        if(midName != null) this.midName = midName;
        else this.midName = "";
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        if(lastName != null) this.lastName = lastName;
        else this.lastName = "";
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(firstName);
        out.writeUTF(midName);
        out.writeUTF(lastName);
    }

    public void readFields(DataInput in) throws IOException {
        firstName = in.readUTF();
        midName = in.readUTF();
        lastName = in.readUTF();
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + midName.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof VisitorTuple)) return false;
        VisitorTuple visitor = (VisitorTuple) o;

        return visitor.firstName.equals(firstName) &&
                visitor.midName.equals(midName) &&
                visitor.lastName.equals(lastName);
    }

    @Override
    public String toString() {
        return firstName + " " + midName + " " + lastName;
    }

    public int compareTo(VisitorTuple v) {
        int last = lastName.compareTo(v.lastName);
        if(last!=0) return last;
        int first = firstName.compareTo(v.firstName);
        if(first!=0) return first;
        return midName.compareTo(v.midName);
    }

}