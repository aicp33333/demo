package com.rpdemo.utils;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * author: houying
 * date  : 16-10-25
 * desc  :
 */
public final class MRUtils {
    public static Pattern SPLITTER = Pattern.compile("\t");
    public static Joiner JOINER = Joiner.on("\t").useForNull("");

    public static String join(Iterable<String> iterable, String sep) {
        if (sep.equals("\t")) {
            return JOINER.join(iterable);
        } else {
            return Joiner.on(sep).useForNull("").join(iterable);
        }
    }

    public static String join(String[] iterable, String sep) {
        if (sep.equals("\t")) {
            return JOINER.join(iterable);
        } else {
            return Joiner.on(sep).useForNull("").join(iterable);
        }
    }

    public static Map<String, String> splitToMap(String str, String sep, String keyValueSep) {
        return Splitter.on(sep).withKeyValueSeparator(keyValueSep).split(str);
    }

    public static String joinMapToString(Map<String, String> map, String rep, String keyValueSep) {
        return Joiner.on(rep).withKeyValueSeparator(keyValueSep).join(map);
    }
}
