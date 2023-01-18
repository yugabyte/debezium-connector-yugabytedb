package io.debezium.connector.yugabytedb.connection;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.postgresql.core.BaseConnection;
import org.postgresql.core.Parser;
//import org.postgresql.jdbc.ArrayDecoding;
//import org.postgresql.jdbc.ArrayEncoding;
import org.postgresql.jdbc.PgArray;
import org.postgresql.util.GT;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class YBArray extends PgArray {
    private final int oid;
    protected @Nullable String fieldString;
    protected @Nullable YBArrayList arrayList;
    protected byte @Nullable [] fieldBytes;
    public YBArray (int oid, String fieldString) throws SQLException {
        super(null, oid, fieldString);
        this.oid = oid;
        this.fieldString = fieldString;
    }

    protected synchronized YBArrayList buildArrayList(String fieldString) throws SQLException {
        if (this.arrayList == null) {
            // TODO Vaibhav: You may need to make the delimiter call dependent on oid
            // getConnection().getTypeInfo().getArrayDelimiter(this.oid)
            this.arrayList = this.buildArrayList(fieldString, ',' /* passing default delimiter as comma */);
        }

        return this.arrayList;
    }

    @Override
    public Object getArray() throws SQLException {
        return this.getArrayImpl(1L, 0, null);
    }

//    @Override
//    public @Nullable Object getArrayImpl(long index, int count, @Nullable Map<String, Class<?>> map) throws SQLException {
//        if (this.fieldString == null) {
//            return null;
//        }
//
//        YBArrayList arrayList = this.buildArrayList(this.fieldString);
//        if (count == 0) {
//            count = arrayList.size();
//        }
//
//        if (index - 1L + (long)count > (long)arrayList.size()) {
//            throw new PSQLException(GT.tr("The array index is out of range: {0}, number of elements: {1}.", new Object[]{index + (long)count, (long)arrayList.size()}), PSQLState.DATA_ERROR);
//        } else {
//            return this.buildArray(arrayList, (int)index, count);
//        }
//    }

    protected YBArrayList buildArrayList(String fieldString, char delim) {
        YBArrayList arrayList = new YBArrayList();
        if (fieldString == null) {
            return arrayList;
        } else {
            char[] chars = fieldString.toCharArray();
            StringBuilder buffer = null;
            boolean insideString = false;
            boolean wasInsideString = false;
            List<YBArrayList> dims = new ArrayList();
            YBArrayList curArray = arrayList;
            int startOffset = 0;
            if (chars[0] == '[') {
                while(chars[startOffset] != '=') {
                    ++startOffset;
                }

                ++startOffset;
            }

            for(int i = startOffset; i < chars.length; ++i) {
                if (chars[i] == '\\') {
                    ++i;
                } else {
                    if (!insideString && chars[i] == '{') {
                        if (dims.isEmpty()) {
                            dims.add(arrayList);
                        } else {
                            YBArrayList a = new YBArrayList();
                            YBArrayList p = (YBArrayList) dims.get(dims.size() - 1);
                            p.add(a);
                            dims.add(a);
                        }

                        curArray = (YBArrayList) dims.get(dims.size() - 1);

                        for(int t = i + 1; t < chars.length; ++t) {
                            if (!Character.isWhitespace(chars[t])) {
                                if (chars[t] != '{') {
                                    break;
                                }

                                ++curArray.dimensionsCount;
                            }
                        }

                        buffer = new StringBuilder();
                        continue;
                    }

                    if (chars[i] == '"') {
                        insideString = !insideString;
                        wasInsideString = true;
                        continue;
                    }

                    if (!insideString && Parser.isArrayWhiteSpace(chars[i])) {
                        continue;
                    }

                    if (!insideString && (chars[i] == delim || chars[i] == '}') || i == chars.length - 1) {
                        if (chars[i] != '"' && chars[i] != '}' && chars[i] != delim && buffer != null) {
                            buffer.append(chars[i]);
                        }

                        String b = buffer == null ? null : buffer.toString();
                        if (b != null && (!b.isEmpty() || wasInsideString)) {
                            curArray.add(!wasInsideString && b.equals("NULL") ? null : b);
                        }

                        wasInsideString = false;
                        buffer = new StringBuilder();
                        if (chars[i] == '}') {
                            dims.remove(dims.size() - 1);
                            if (!dims.isEmpty()) {
                                curArray = (YBArrayList) dims.get(dims.size() - 1);
                            }

                            buffer = null;
                        }
                        continue;
                    }
                }

                if (buffer != null) {
                    buffer.append(chars[i]);
                }
            }

            return arrayList;
        }
    }

//    @Override
//    public @Nullable String toString() {
//        if (this.fieldString == null && this.fieldBytes != null) {
//            try {
//                Object array = super.readBinaryArray(this.fieldBytes, 1, 0);
//                ArrayEncoding.ArrayEncoder arraySupport = ArrayEncoding.getArrayEncoder(array);
//
//                assert arraySupport != null;
//
//                this.fieldString = arraySupport.toArrayString(',' /*this.connection.getTypeInfo().getArrayDelimiter(this.oid)*/, array);
//            } catch (SQLException var3) {
//                this.fieldString = "NULL";
//            }
//        }
//
//        return this.fieldString;
//    }

    static final class YBArrayList extends ArrayList<@Nullable Object> {
        private static final long serialVersionUID = 1L;
        int dimensionsCount = 1;

        YBArrayList() {
        }
    }
}
