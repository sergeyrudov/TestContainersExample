package data;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class JoinedDataRow<K, V1, V2> {
    private K key;
    private V1 value1;
    private V2 value2;

}