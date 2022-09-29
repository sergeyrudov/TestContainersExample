package ops;

import data.DataRow;
import data.JoinedDataRow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * --Short description--
 *  1. get key from right table, and compare it with key in left table
 *  2. if keys equals, return joined data in format:
 *  3. key from right table, result from left table, value2 from left table
 *  --------------------------------------------------------------
 *  4. if keys are not equal, return joined data in format:
 *  5. key from right table, null, value2 from right table
 */
public class RightJoinOperation<K, V1, V2> implements JoinOperation<DataRow<K,V1>, DataRow<K,V2>, JoinedDataRow<K,V1,V2>> {

    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection,
                                                     Collection<DataRow<K, V2>> rightCollection) {

        if(leftCollection == null || rightCollection == null) {
            throw new IllegalArgumentException("Collection cannot be null");
        }

        List<JoinedDataRow<K, V1, V2>> list = new ArrayList<>();

        rightCollection
                .forEach(rightDataRow -> {
                    list.add(new JoinedDataRow<>(rightDataRow.getKey(),
                            leftCollection.stream()
                                    .filter(leftDataRow -> leftDataRow.getKey() == rightDataRow.getKey())
                                    .map(DataRow::getValue)
                                    .findFirst()
                                    .orElse(null),
                            rightDataRow.getValue()
                    ));
                });
        return list;
    }
}