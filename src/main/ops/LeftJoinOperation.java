package ops;

import data.DataRow;
import data.JoinedDataRow;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *  --Short description--
 *  1. get key from left table, and compare it with key in right table
 *  2. if keys equals, return joined data in format:
 *  3. key from left table, value1 from left table, value2 from right table
 *  --------------------------------------------------------------
 *  4. if keys are not equal, return joined data in format:
 *  5. key from left table, value1 from left table, null
 */
public class LeftJoinOperation<K, V1, V2> implements JoinOperation<DataRow<K, V1>, DataRow<K, V2>, JoinedDataRow<K, V1, V2>> {
    @Override
    public Collection<JoinedDataRow<K, V1, V2>> join(Collection<DataRow<K, V1>> leftCollection, Collection<DataRow<K, V2>> rightCollection) {

        if (leftCollection == null || rightCollection == null) {
            throw new IllegalArgumentException("Collection cannot be null");
        }

        List<JoinedDataRow<K, V1, V2>> list = new ArrayList<>();

        leftCollection.forEach(dataRow1 -> {
            boolean[] keyAvailable = {false};

            rightCollection.stream()
                    .filter(dataRow2 -> dataRow1.getKey() == dataRow2.getKey())
                    .forEach(dataRow2 -> {
                        list.add(new JoinedDataRow<>(dataRow1.getKey(), dataRow1.getValue(), dataRow2.getValue()));
                        keyAvailable[0] = true;
                });

            if (!keyAvailable[0]) {
                list.add(new JoinedDataRow<>(dataRow1.getKey(), dataRow1.getValue(), null));
            }
        });
        return list;

    }
}