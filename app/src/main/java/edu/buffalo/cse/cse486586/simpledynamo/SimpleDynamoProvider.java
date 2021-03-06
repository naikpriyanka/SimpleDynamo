package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentUris;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoDbHelper;
import edu.buffalo.cse.cse486586.simpledynamo.model.Message;
import edu.buffalo.cse.cse486586.simpledynamo.model.MessageType;

import static android.content.Context.TELEPHONY_SERVICE;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.BASE_CONTENT_URI;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.KEY_FIELD;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.TABLE_NAME;
import static edu.buffalo.cse.cse486586.simpledynamo.data.SimpleDynamoContract.SimpleDhtEntry.VALUE_FIELD;
import static edu.buffalo.cse.cse486586.simpledynamo.model.Message.DELIMITER;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.DELETE;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.DELETE_ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.INSERT;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.QUERY;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.QUERY_ALL;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.RECOVER;
import static edu.buffalo.cse.cse486586.simpledynamo.model.MessageType.getEnumBy;

/*
 * References :
 * Ethan Blanton slides on Concurrency Control, Consistency, Amazon Dynamo
 * Amazon Dynamo : https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
 * Chain Replication : http://www.cs.cornell.edu/home/rvr/papers/OSDI04.pdf
 */

public class SimpleDynamoProvider extends ContentProvider {

    public static final String LOG_TAG = SimpleDynamoProvider.class.getSimpleName();

    private SimpleDynamoDbHelper mDbHelper;

    private ContentResolver mContentResolver;

    private static final int SERVER_PORT = 10000;
    private static final String LDUMP = "@"; //Specific AVD
    private static final String GDUMP = "*"; //All AVDs
    private String selfPort = null; //Used to store the port number of the AVD
    private Map<String, String> lookupTable = new TreeMap<String, String>(); //Formation of ring in order of their hash values
    private String[] portTable; //Storing ports in order
    private String[] hashTable; //Storing hash values of port

    //Make two columns for key and value
    private static final String COLUMN_NAMES[] = new String[]{KEY_FIELD, VALUE_FIELD};

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        int rowsDeleted = 0;

        //Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        if (LDUMP.equals(selection)) {
            //Delete all the msgs from a particular node
            rowsDeleted = database.delete(TABLE_NAME, null, null);
        } else if (GDUMP.equals(selection)) {
            //Delete all the msgs from each node, one at a time by connecting remotely
            Message msg = new Message(DELETE_ALL);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString());
        } else {
            try {
                String hashedKey = genHash(selection);  //Get hash of the key to find the node where the key is situated
                String nextPort = getSuccessorFrom(hashedKey); //Get the successor port from the hash value
                if (selfPort.equals(nextPort)) { //Check if the successor port is equal to self port
                    rowsDeleted = deleteFromDB(selection, selectionArgs);
                }
                //Delete the key from all the successors
                Message msg = new Message(DELETE);
                msg.setKey(selection);
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), nextPort);
            } catch (NoSuchAlgorithmException e) {
                Log.e(LOG_TAG, "Error in generating hash for the key " + selection);
            }
        }
        return rowsDeleted;
    }

    @Override
    public String getType(Uri uri) {
        // COMPLETED Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        //Check that the key is not null
        String key = values.getAsString(KEY_FIELD);
        if (key == null) {
            throw new IllegalArgumentException("Message requires a key");
        }

        //Check that the value is not null
        String value = values.getAsString(VALUE_FIELD);
        if (value == null) {
            throw new IllegalArgumentException("Message requires a value");
        }
        try {
            String hashedKey = genHash(key); //Get hash of the key to find the node where the key is situated
            String nextPort = getSuccessorFrom(hashedKey); //Get the successor port from the hash value
            if (selfPort.equals(nextPort)) { //Check if the successor port is equal to self port
                insertInDB(uri, values);
            }
            //Replicate data to successors
            Message msg = new Message(INSERT, key, value);
            new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), nextPort);
        } catch (NoSuchAlgorithmException e) {
            Log.e(LOG_TAG, "Error in generating hash for the key " + key);
        }
        return null;
    }

    private Uri insertInDB(Uri uri, ContentValues values) {
        /*
         * Reference: https://developer.android.com/reference/android/database/sqlite/SQLiteDatabase.html#insertWithOnConflict(java.lang.String,%20java.lang.String,%20android.content.ContentValues,%20int)
         */
        //Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        long id = database.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);

        //If the ID is -1, then the insertion failed. Log an error and return null.
        if (id == -1) {
            Log.e(LOG_TAG, "Failed to insert row for " + uri);
            return null;
        }
        //Notify all listeners that the data has changed for the message content URI
        mContentResolver.notifyChange(uri, null);
        //Return the new URI with the ID (of the newly inserted row) appended at the end
        return ContentUris.withAppendedId(uri, id);
    }

    private int deleteFromDB(String selection, String[] selectionArgs) {
        //Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();
        selectionArgs = new String[]{selection};
        selection = KEY_FIELD + "=?";
        return database.delete(TABLE_NAME, selection, selectionArgs);
    }

    @Override
    public boolean onCreate() {
        Context context = this.getContext();
        mContentResolver = context.getContentResolver();
        mDbHelper = new SimpleDynamoDbHelper(context);
        initLookUpTable();

        //Calculate the port number that this AVD listens on
        TelephonyManager tel = (TelephonyManager) context.getSystemService(TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        //Store the current port in self port for future references
        selfPort = portStr;

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             * ServerSocket is a socket which servers can use to listen and accept requests from clients
             * AsyncTask is a simplified thread construct that Android provides.
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(LOG_TAG, "Can't create a ServerSocket");
        }
        //Send recovery message if the node has failed and joined in
        Message msg = new Message(RECOVER);
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg.toString(), portStr);
        return true;
    }

    //Method to create a lookup table in the order of the hash value and place them in two different tables for easy access
    private void initLookUpTable() {
        String[] ports = {"5554", "5556", "5558", "5560", "5562"};
        for(String port : ports) {
            try {
                lookupTable.put(genHash(port), port);
            } catch (NoSuchAlgorithmException e) {
                Log.e(LOG_TAG, "Error in generating hash for the key " + port);
            }
        }
        hashTable = lookupTable.keySet().toArray(new String[0]);
        portTable = lookupTable.values().toArray(new String[0]);
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs, String sortOrder) {
        //Get readable database
        SQLiteDatabase database = mDbHelper.getReadableDatabase();
        Cursor resultCursor;
        if (LDUMP.equals(selection)) {
            //Query all the msgs from a particular node
            resultCursor = database.query(TABLE_NAME, null, null, null, null, null, null);
            return resultCursor;
        } else if (GDUMP.equals(selection)) {
            //Append messages from all the nodes
            StringBuilder output = new StringBuilder();
            //Query all the msgs from each node, one at a time by connecting remotely
            for (String remotePort : portTable) {
                try {
                    //Get the socket with the given port number
                    Socket socket = getSocket(getPortFromLineNumber(remotePort));
                    try {
                        //Create an output data stream to send QUERY message to all the nodes
                        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                        //Create a QUERY_ALL message to be sent to the nodes
                        Message msgToSend = new Message(QUERY_ALL);
                        //Write the message on the output stream
                        out.writeUTF(msgToSend.toString());
                        //Flush the output stream
                        out.flush();
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error writing data to output stream in QUERY_ALL for port " + remotePort);
                    }
                    try {
                        //Create an input data stream to read messages from all the nodes
                        DataInputStream in = new DataInputStream(socket.getInputStream());
                        //Read the message received
                        String msgReceived = in.readUTF();
                        //Append it to the output
                        output.append(msgReceived);
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error reading data from input stream in QUERY_ALL for port " + remotePort);
                    }
                    socket.close();
                } catch (IOException e) {
                    Log.e(LOG_TAG, "Error in socket creation " + remotePort);
                }
            }
            //Get messages as map of key and value from the output returned from all the nodes
            Map<String, String> allMessages = getAllMessages(output.toString());
            /*
             * Reference: https://developer.android.com/reference/android/database/MatrixCursor.html
             *
             * Create Matrix Cursor for placing all the messages
             */
            MatrixCursor matrixCursor = new MatrixCursor(COLUMN_NAMES);
            //Iterate over all the messages and add row for each entry
            for (Map.Entry<String, String> entry : allMessages.entrySet()) {
                //Create key value pair
                String keyValue[] = new String[]{entry.getKey(), entry.getValue()};
                //Add that pair to the matrix cursor
                matrixCursor.addRow(keyValue);
            }
            return matrixCursor;
        } else {
            try {
                String hashedKey = genHash(selection); //Get hash of the key to find the node where the key is situated
                String nextPort = getSuccessorFrom(hashedKey); //Get the port from the hash value
                if (selfPort.equals(nextPort)) { //Check if the successor port is equal to self port
                    //Query selection key from the database
                    selectionArgs = new String[]{selection};
                    selection = KEY_FIELD + "=?";
                    resultCursor = database.query(TABLE_NAME, projection, selection, selectionArgs, null, null, sortOrder);
                    resultCursor.setNotificationUri(mContentResolver, uri);
                    return resultCursor;
                } else {
                    try {
                        //Get the successor node of the port
                        String succ[] = getSuccessorOf(nextPort);
                        /*
                         * Get the socket with the given port number
                         * Start querying from the tail of the list according to chain replication
                         * Query the last replica in the partition
                         */
                        Socket socket = getSocket(getPortFromLineNumber(succ[1]));
                        try {
                            //Create an output data stream to send QUERY message to a particular node
                            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                            //Create a QUERY message to be sent to that node
                            Message msgToSend = new Message(QUERY);
                            //Set the selection as key in the msg
                            msgToSend.setKey(selection);
                            //Write the message on the output stream
                            out.writeUTF(msgToSend.toString());
                            //Flush the output stream
                            out.flush();
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error writing data to output stream in QUERY for port " + nextPort);
                        }

                        String msgReceived;
                        try {
                            //Create an input data stream to read messages from a particular node
                            DataInputStream in = new DataInputStream(socket.getInputStream());
                            try {
                                //Read the message received
                                msgReceived = in.readUTF();
                                if (msgReceived.isEmpty()) {
                                    //If msg received is empty then query the second last replica in the partition
                                    msgReceived = queryReplicas(succ[0], selection, nextPort);
                                }
                            } catch (Exception e) {
                                //If there is exception while reading message then query the second last replica in the partition
                                msgReceived = queryReplicas(succ[0], selection, nextPort);
                            }
                            //Create Matrix Cursor for placing all the messages from that node
                            MatrixCursor matrixCursor = new MatrixCursor(COLUMN_NAMES);
                            //Create key value pair
                            String keyValue[] = {selection, msgReceived};
                            //Add that pair to the matrix cursor
                            matrixCursor.addRow(keyValue);
                            return matrixCursor;
                        } catch (IOException e) {
                            Log.e(LOG_TAG, "Error reading data from input stream in QUERY for port " + nextPort);
                        }
                        socket.close();
                    } catch (IOException e) {
                        Log.e(LOG_TAG, "Error in socket creation" + nextPort);
                    }
                }
            } catch (NoSuchAlgorithmException e) {
                Log.e(LOG_TAG, "Error in generating hash for the key " + selection);
            }
        }
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // COMPLETED Auto-generated method stub
        return 0;
    }

    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            /*
             * an iterative server that can service multiple clients, though, one at a time.
             */
            while (true) {
                try {
                    if (serverSocket != null) {
                        Socket server = serverSocket.accept();
                        DataInputStream in = new DataInputStream(new BufferedInputStream(server.getInputStream()));
                        //Get message from the input stream
                        String msgReceived = in.readUTF();
                        String[] msgPacket = msgReceived.split(DELIMITER);
                        //Get the message type from the message string
                        MessageType msgType = getEnumBy(msgPacket[0]);
                        if (msgType != null) {
                            switch (msgType) {
                                case INSERT:
                                    //Insert the key and the value in the database
                                    ContentValues values = new ContentValues();
                                    values.put(KEY_FIELD, msgPacket[1]);
                                    values.put(VALUE_FIELD, msgPacket[2]);
                                    insertInDB(BASE_CONTENT_URI, values);
                                    break;

                                case QUERY:
                                    SQLiteDatabase database = mDbHelper.getReadableDatabase();
                                    String selection = msgPacket[1];
                                    //Create an output data stream to send retrieved QUERY message back to the querying node
                                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                                    //Query the database with the selection key
                                    String[] selectionArgs = new String[]{selection};
                                    selection = KEY_FIELD + "=?";
                                    Cursor cursor = database.query(TABLE_NAME, null, selection, selectionArgs, null, null, null);
                                    String value = null;
                                    /*
                                     * Move the cursor to the first record
                                     *
                                     * Reference: https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                                     */
                                    if (cursor.moveToFirst()) {
                                        //Get the value from the column
                                        value = cursor.getString(cursor.getColumnIndex(VALUE_FIELD));
                                    }
                                    if (value != null) {
                                        out.writeUTF(value);
                                    }
                                    out.flush();
                                    break;

                                case QUERY_ALL:
                                    SQLiteDatabase sqlDatabase = mDbHelper.getReadableDatabase();
                                    //Create an output data stream to send retrieved QUERY_ALL message back to the querying node
                                    DataOutputStream out1 = new DataOutputStream(server.getOutputStream());
                                    //Query the database to get all the results on that node
                                    Cursor resultCursor = sqlDatabase.query(TABLE_NAME, null, null, null, null, null, null);
                                    /*
                                     * Reference: https://developer.android.com/reference/android/database/MatrixCursor.html#getCount()
                                     *
                                     * Get column count
                                     */
                                    int numColumns = resultCursor.getColumnCount();
                                    StringBuilder output = new StringBuilder();
                                    /*
                                     * Move the cursor to the -1 position
                                     * In order to iterate over the result cursor returned from the sql database query
                                     * I move the cursor with -1 position so that I can directly use the function moveToNext
                                     * in the while loop
                                     *
                                     * Reference: https://developer.android.com/reference/android/database/Cursor.html
                                     * Reference: https://stackoverflow.com/questions/10723770/whats-the-best-way-to-iterate-an-android-cursor
                                     */
                                    resultCursor.moveToPosition(-1);
                                    //Iterate over all the values in the cursor
                                    while (resultCursor.moveToNext()) {
                                        for (int i = 0; i < numColumns; i++) {
                                            output.append(resultCursor.getString(i)).append(DELIMITER);
                                        }
                                    }
                                    out1.writeUTF(output.toString());
                                    out1.flush();
                                    break;

                                case DELETE:
                                    deleteFromDB(msgPacket[1], null);
                                    break;

                                case DELETE_ALL:
                                    deleteFromDB(null, null);
                                    break;

                                default:
                                    throw new IllegalArgumentException("Unknown Message type" + msgType);
                            }
                        } else {
                            throw new IllegalArgumentException("Message type is null");
                        }
                    } else {
                        Log.e(LOG_TAG, "The server socket is null");
                    }
                } catch (IOException e) {
                    Log.e(LOG_TAG, "Error accepting socket" + e);
                }
            }
        }
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgPacket = msgs[0].split(DELIMITER);
            MessageType msgType = getEnumBy(msgPacket[0]);
            String remotePort;
            String[] succ;
            if (msgType != null) {
                switch (msgType) {
                    case RECOVER:
                        //Recover the given port
                        remotePort = msgs[1];
                        recover(remotePort);
                        break;

                    case INSERT:
                        remotePort = msgs[1];
                        //Get the successor ports of the given port
                        succ = getSuccessorOf(remotePort);
                        //If the remote port is not equal to self then connect remotely and insert data in database
                        if (!selfPort.equals(remotePort)) {
                            insertTo(msgPacket, remotePort);
                        }
                        //Insert data in replicas
                        for (String succPort : succ) {
                            insertTo(msgPacket, succPort);
                        }
                        break;

                    case DELETE:
                        remotePort = msgs[1];
                        //Get the successor ports of the given port
                        succ = getSuccessorOf(remotePort);
                        //If the remote port is not equal to self then connect remotely and delete data in database
                        if (!selfPort.equals(remotePort)) {
                            deletionFrom(msgPacket[1], remotePort);
                        }
                        //Delete data from replicas
                        for (String succPort : succ) {
                            deletionFrom(msgPacket[1], succPort);
                        }
                        break;

                    case DELETE_ALL:
                        //Delete all the messages from all the ports
                        for (String remotePort2 : portTable) {
                            deleteAllFrom(remotePort2);
                        }
                        break;

                    default:
                        throw new IllegalArgumentException("Unknown Message type" + msgType);

                }
            } else {
                throw new IllegalArgumentException("Message type is null");
            }
            return null;
        }
    }

    private void deleteAllFrom(String remotePort2) {
        try {
            //Get the socket with the given port number
            Socket socket = getSocket(getPortFromLineNumber(remotePort2));
            //Create an output data stream to send DELETE_ALL message to a particular node
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Create DELETE msg with key to delete all the message from all the nodes node
            Message msgToSend = new Message(DELETE_ALL);
            //Write the msg on the output stream
            out.writeUTF(msgToSend.toString());
            //Flush the output stream
            out.flush();
            socket.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Error in writing DELETE_ALL on port " + remotePort2);
        }
    }

    private void insertTo(String[] msgPacket, String port) {
        try {
            //Get the socket with the given port number
            Socket socket = getSocket(getPortFromLineNumber(port));
            //Create an output data stream to send INSERT message to a particular node
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Create INSERT msg with key and value
            Message msgToSend = new Message(INSERT, msgPacket[1], msgPacket[2]);
            //Write msg on the output stream
            out.writeUTF(msgToSend.toString());
            //Flush the output stream
            out.flush();
            socket.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Error in writing INSERT on port " + port + e);
        }
    }

    private void deletionFrom(String key, String port) {
        try {
            //Get the socket with the given port number
            Socket socket = getSocket(getPortFromLineNumber(port));
            //Create an output data stream to send DELETE message to a particular node
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            //Create DELETE msg with key to delete all the message from that node
            Message msgToSend = new Message(DELETE);
            msgToSend.setKey(key);
            //Write the msg on the output stream
            out.writeUTF(msgToSend.toString());
            //Flush the output stream
            out.flush();
            socket.close();
        } catch (IOException e) {
            Log.e(LOG_TAG, "Error in writing DELETE on port " + port);
        }
    }

    /*
     * References for Transaction :
     * https://medium.com/inloopx/transactions-and-threads-in-sqlite-on-android-215e46670f2d
     * https://www.sqlite.org/isolation.html
     * http://www.vogella.com/tutorials/AndroidSQLite/article.html
     */
    //Method to recover data for the given port
    private void recover(String port) {
        //Get writable database
        SQLiteDatabase database = mDbHelper.getWritableDatabase();

        //SQLite locks the database during a transaction
        database.beginTransaction();
        //Delete all the previous messages stored in the database of the failed node
        int rowsDeleted = database.delete(TABLE_NAME, null, null);
        //If the rows deleted from the database is not zero then the node was failed and recovered
        if (rowsDeleted != 0) {
            //Recover the messages from the replica nodes and messages for which the node itself was replica
            //Get the successor nodes of the recovered port
            String succ[] = getSuccessorOf(port);
            //Get the predecessor nodes of the recovered port
            String pred[] = getPredecessorOf(port);
            //Place them in an array for easy access
            String[] recoveryPorts = {succ[0], succ[1], pred[0], pred[1]};
            //Create a map to store key-value retrieved from all the nodes
            Map<String, String> map = new HashMap<String, String>();
            //Iterate over each port from which it can recover
            for (String remotePort : recoveryPorts) {
                try {
                    //Connect to the port
                    Socket socket = getSocket(getPortFromLineNumber(remotePort));
                    //Get all the messages present at that node
                    Message msgToSend = new Message(QUERY_ALL);
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(msgToSend.toString());
                    out.flush();

                    //Get the message from the node
                    DataInputStream in = new DataInputStream(socket.getInputStream());
                    String op = in.readUTF();
                    //Put it in the map
                    map.putAll(getAllMessages(op));
                    socket.close();
                } catch (IOException e) {
                    Log.e(LOG_TAG, "Error in socket creation " + remotePort);
                }
            }
            //Iterate over the map to get the exact location of the msg and place it there
            for (Map.Entry<String, String> m : map.entrySet()) {
                //Get the key and value from the map
                String key = m.getKey();
                String value = m.getValue();
                try {
                    String hashedKey = genHash(key); //Get hash of the key to find the node where the key is situated
                    String nextPort = getSuccessorFrom(hashedKey); //Get the port from the hash value
                    //Restore the data for the given port as well as for the predecessor because the port acted as replica for its predecessors
                    //So the message that belongs to its predecessors should also be saved as replica for predecessors
                    if (nextPort.equals(port) || nextPort.equals(pred[0]) || nextPort.equals(pred[1])) {
                        ContentValues values = new ContentValues();
                        values.put(KEY_FIELD, key);
                        values.put(VALUE_FIELD, value);
                        database.insertWithOnConflict(TABLE_NAME, null, values, SQLiteDatabase.CONFLICT_REPLACE);
                    }
                } catch (NoSuchAlgorithmException e) {
                    Log.e(LOG_TAG, "Error in generating hash for the key " + key);
                }
            }
        }
        database.setTransactionSuccessful();
        database.endTransaction();
    }

    //Method to create socket for the given port
    private Socket getSocket(String remotePort) throws IOException {
        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
        socket.setTcpNoDelay(true);
        socket.setSoTimeout(1000);
        return socket;
    }

    //Method to get the TCP port from the Line Number
    private String getPortFromLineNumber(String portStr) {
        return String.valueOf(Integer.parseInt(portStr) * 2);
    }

    //Method to get the appropriate port for the key
    private String getSuccessorFrom(String hashKey) {
        for (int i = 0; i < portTable.length; i++) {
            if (hashTable[i].compareTo(hashKey) > 0) { //Compare the hash values to get the successor port
                return portTable[i];
            }
        }
        /*
         * If the value greater than the hash is not found
         * then either the key hash greater than hash of last node in the ring
         * or the key hash less than hash of first node in the ring
         */
        return portTable[0];
    }

    //Method to separate out key and value from the message
    private Map<String, String> getAllMessages(String s) {
        Map<String, String> messages = new HashMap<String, String>();
        String[] msgPackets = s.split(DELIMITER);
        if (msgPackets.length >= 2) {
            for (int i = 0; i < msgPackets.length; i += 2) {
                messages.put(msgPackets[i], msgPackets[i + 1]);
            }
        }
        return messages;
    }

    //Method to get the two successor of the port
    private String[] getSuccessorOf(String port) {
        int index = search(port);
        String succ[] = new String[2];
        if (index != -1) {
            succ[0] = portTable[(index + 1) % portTable.length];
            succ[1] = portTable[(index + 2) % portTable.length];
        }
        return succ;
    }

    //Method to get the position of the node in the ring
    private int search(String port) {
        for (int i = 0; i < portTable.length; i++) {
            if (portTable[i].equals(port)) {
                return i;
            }
        }
        return -1;
    }

    //Method to get the two predecessor of the port
    private String[] getPredecessorOf(String port) {
        String pred[] = new String[2];
        int index = search(port);
        if (index != -1) {
            pred[0] = portTable[(index + portTable.length - 1) % portTable.length];
            pred[1] = portTable[(index + portTable.length - 2) % portTable.length];
        }
        return pred;
    }

    //Method to query the replicas
    private String queryReplicas(String replicaPort1, String selection, String replicaPort2) {
        String output = null;
        try {
            Socket socket = getSocket(getPortFromLineNumber(replicaPort1));
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            Message msgToSend = new Message(QUERY);
            msgToSend.setKey(selection);
            out.writeUTF(msgToSend.toString());
            out.flush();

            DataInputStream in = new DataInputStream(socket.getInputStream());
            try {
                //Read the message received
                output = in.readUTF();
                //If the message retrieved from the port is empty
                if (output.isEmpty()) {
                    //Query the next replica in the partition
                    output = queryReplicas(replicaPort2, selection, replicaPort2);
                }
            } catch (Exception e) {
                //In case of failure to read message from the port, query the next replica in the partition
                output = queryReplicas(replicaPort2, selection, replicaPort2);
            }
        } catch (Exception e) {
            Log.e(LOG_TAG, "Error in socket creation " + replicaPort1);
        }
        return output;
    }
}

