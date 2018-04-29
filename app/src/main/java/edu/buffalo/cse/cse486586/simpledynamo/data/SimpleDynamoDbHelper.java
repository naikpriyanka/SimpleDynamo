package edu.buffalo.cse.cse486586.simpledynamo.data;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.TABLE_NAME;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.VALUE_FIELD;

/**
 * Created by priyankanaik on 12/02/2018.
 */

public class SimpleDynamoDbHelper extends SQLiteOpenHelper {

    /*
     * Name of the database file
     */
    private static final String DATABASE_NAME = "simple_dynamo.db";

    /**
     * Database version. If you change the database schema, you must increment the database version.
     */
    private static final int DATABASE_VERSION = 1;

    /**
     * Constructs a new instance of {@link SimpleDynamoDbHelper}.
     *
     * @param context of the app
     */
    public SimpleDynamoDbHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        // Create a String that contains the SQL statement to create the messages table
        String SQL_CREATE_MESSAGES_TABLE = "CREATE TABLE " + TABLE_NAME + " ("
                + KEY_FIELD + " TEXT NOT NULL UNIQUE, "
                + VALUE_FIELD + " TEXT NOT NULL);";

        // Execute the SQL statement
        db.execSQL(SQL_CREATE_MESSAGES_TABLE);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // drop the old table
        db.execSQL("DROP TABLE IF EXISTS " + TABLE_NAME);
        //Create the table again
        onCreate(db);
    }
}
