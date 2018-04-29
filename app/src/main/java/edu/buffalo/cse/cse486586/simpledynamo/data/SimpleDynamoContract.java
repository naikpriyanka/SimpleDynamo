package edu.buffalo.cse.cse486586.simpledynamo.data;

import android.net.Uri;
import android.provider.BaseColumns;

/**
 * Created by priyankanaik on 12/02/2018.
 */

public final class SimpleDynamoContract {

    public static final String CONTENT_AUTHORITY = "edu.buffalo.cse.cse486586.simpledynamo.provider";

    public static final Uri BASE_CONTENT_URI = Uri.parse("content://" + CONTENT_AUTHORITY);

    /**
     * Inner class that defines constant values for the messages database table.
     * Each entry in the table represents a single message.
     */
    public static final class SimpleDhtEntry implements BaseColumns {

        /** Name of database table for messages */
        public final static String TABLE_NAME = "simple_dynamo";

        /**
         * Unique ID string for the message. It will be used for retrieving messages
         *
         * Type: TEXT
         */
        public final static String KEY_FIELD = "key";

        /**
         * Message value
         *
         * Type: TEXT
         */
        public final static String VALUE_FIELD = "value";

    }
}
