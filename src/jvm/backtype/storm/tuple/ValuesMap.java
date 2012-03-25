package backtype.storm.tuple;

import java.util.Arrays;
import java.util.Map;
import java.util.List;
import java.util.Set;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.io.Serializable;
import java.lang.UnsupportedOperationException;
import java.lang.IllegalArgumentException;
import java.lang.ClassCastException;
import backtype.storm.tuple.Fields;

public class ValuesMap extends Object implements Map, Serializable {
    private LinkedHashMap<String, Object> _map;

    public ValuesMap(String... fields) {
        this(Arrays.asList(fields));
    }

    public ValuesMap(List<String> fields) {
        _map = new LinkedHashMap<String, Object>(fields.size());

        for (String field : fields)
            _map.put(field, null);
    }

    public ValuesMap(Fields fields) {
        this(fields.toList());
    }

    public void clear() {
        throw new UnsupportedOperationException();
    }

    public boolean containsKey(Object key) {
        return _map.containsKey(key);
    }

    public boolean containsValue(Object value) {
        return _map.containsValue(value);
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        return _map.entrySet();
    }

    public Object get(Object key) {
        String field;
        try {
            field = (String) key;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("keys must be String");
        }

        if (_map.containsKey(field)) {
            return _map.get(field);
        } else {
            String msg = String.format("instance does not contain field '%s'", field);
            throw new IllegalArgumentException(msg);
        }
    }

    public boolean isEmpty() {
        return _map.isEmpty();
    }

    public Set<String> keySet() {
        return _map.keySet();
    }

    public Object put(Object key, Object value) {
        String field;
        try {
            field = (String) key;
        } catch (ClassCastException e) {
            throw new IllegalArgumentException("keys must be String");
        }

        if (_map.containsKey(field)) {
            return _map.put(field, value);
        } else {
            String msg = String.format("instance does not contain field '%s'", field);
            throw new IllegalArgumentException(msg);
        }
    }

    public void putAll(Map map) {
        for (Object entry : map.entrySet()) {
            Map.Entry map_entry = (Map.Entry) entry;
            this.put(map_entry.getKey(), map_entry.getValue());
        }
    }

    public Object remove(Object key) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return _map.size();
    }

    public Collection<Object> values() {
        return _map.values();
    }
}
