package bundle.helpers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class IteratorHelper {
    private IteratorHelper() {}

    /**
     * Convert an iterator into a list.
     */
    public static <T> List<T> toList(Iterator<T> iterator) {
        List<T> list = new ArrayList<>();
        iterator.forEachRemaining(list::add);
        return list;
    }
}
