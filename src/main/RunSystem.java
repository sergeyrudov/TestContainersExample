import com.google.common.collect.ImmutableList;
import data.DataRow;
import data.JoinedDataRow;
import ops.InnerJoinOperation;
import ops.JoinOperation;
import ops.LeftJoinOperation;
import ops.RightJoinOperation;

import java.util.Collection;

public class RunSystem {
    public static void main(String[] args) {

        // prepare collections
        Collection<DataRow<Integer, String>> leftCollection = ImmutableList.of(
                new DataRow<>(0, "Ukraine"),
                new DataRow<>(1, "Germany"),
                new DataRow<>(2, "France")
        );

        Collection<DataRow<Integer, String>> rightCollection = ImmutableList.of(
                new DataRow<>(0, "Kyiv"),
                new DataRow<>(1, "Berlin"),
                new DataRow<>(3, "Budapest")
        );

        // prepare variable for result
        JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>, JoinedDataRow<Integer, String, String>> operation;

        // result for inner join operation
        operation = new InnerJoinOperation<>();
        System.out.println("Result of inner join: " + operation.join(leftCollection, rightCollection) + "\n");

        // result for left join operation
        operation = new LeftJoinOperation<>();
        System.out.println("Result of left join: " + operation.join(leftCollection, rightCollection) + "\n");

        // result for right join operation
        operation = new RightJoinOperation<>();
        System.out.println("Result of right join: " + operation.join(leftCollection, rightCollection) + "\n");

    }
}
