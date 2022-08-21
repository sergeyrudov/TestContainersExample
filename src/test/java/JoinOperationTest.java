import com.google.common.collect.ImmutableList;
import data.DataRow;
import data.JoinedDataRow;
import ops.InnerJoinOperation;
import ops.JoinOperation;
import ops.LeftJoinOperation;
import ops.RightJoinOperation;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class JoinOperationTest {
    private static Collection<DataRow<Integer, String>> leftCollection;
    private static Collection<DataRow<Integer, String>> rightCollection;

    @BeforeAll
    public static void initTestData() {
        leftCollection = ImmutableList.of(
                new DataRow<>(0, "Ukraine"),
                new DataRow<>(1, "Germany"),
                new DataRow<>(2, "France")
        );

        rightCollection = ImmutableList.of(
                new DataRow<>(0, "Kyiv"),
                new DataRow<>(1, "Berlin"),
                new DataRow<>(3, "Budapest")
        );

    }

    @ParameterizedTest(name = "Check that data from left and right collections, are joined correctly")
    @MethodSource("prepareExpectedResult")
    public void checkJoinOperations(String joinType, Collection<JoinedDataRow<Integer, String, String>> expectedResult) {

        JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>,
                JoinedDataRow<Integer, String, String>> operation;

        Collection<JoinedDataRow<Integer, String, String>> result;

        switch (joinType) {
            case "innerJoin":
                operation = new InnerJoinOperation<>();
                result = operation.join(leftCollection, rightCollection);
                assertThat(result).as("type of join " + joinType).isEqualTo(expectedResult);
                break;

            case "leftJoin":
                operation = new LeftJoinOperation<>();
                result = operation.join(leftCollection, rightCollection);
                assertThat(result).as("type of join " + joinType).isEqualTo(expectedResult);
                break;

            case "rightJoin":
                operation = new RightJoinOperation<>();
                result = operation.join(leftCollection, rightCollection);
                assertThat(result).as("type of join " + joinType).isEqualTo(expectedResult);
                break;
        }
    }

    @Test
    public void checkInputNullAsInput() {
        JoinOperation<DataRow<Integer, String>, DataRow<Integer, String>,
                JoinedDataRow<Integer, String, String>> operation = new InnerJoinOperation<>();

        assertThrows(IllegalArgumentException.class,
                () -> operation.join(null, null), "Error, collection cannot be null");
    }


    private static Stream<Arguments> prepareExpectedResult() {
        return Stream.of(
                Arguments.of("innerJoin", ImmutableList.of(
                        new JoinedDataRow<>(0, "Ukraine", "Kyiv"),
                        new JoinedDataRow<>(1, "Germany", "Berlin"))
                ),

                Arguments.of("leftJoin", ImmutableList.of(
                        new JoinedDataRow<>(0, "Ukraine", "Kyiv"),
                        new JoinedDataRow<>(1, "Germany", "Berlin"),
                        new JoinedDataRow<>(2, "France", null)
                        )
                ),

                Arguments.of("rightJoin", ImmutableList.of(
                        new JoinedDataRow<>(0, "Ukraine", "Kyiv"),
                        new JoinedDataRow<>(1, "Germany", "Berlin"),
                        new JoinedDataRow<>(3, null , "Budapest")
                        )
                )
        );
    }



}





