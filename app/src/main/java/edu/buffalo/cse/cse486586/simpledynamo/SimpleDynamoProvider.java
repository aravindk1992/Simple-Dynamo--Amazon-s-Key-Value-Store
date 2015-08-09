package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import android.content.ContentProvider;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.DatabaseUtils;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
    FeedReaderDBHelper mDbHelper;
    String portStr = null;
    String myPort = null;
    String pred = null;
    String pred1 = null;
    String succ = null;
    String succ1 = null;
    int SERVER_PORT = 10000;
    String HashIt = null;

    volatile boolean status= false;
    volatile boolean flag = false;
    ContentValues vals = new ContentValues();
    MatrixCursor m = null;
    volatile int darkside =0;
    volatile boolean busywait = true;
    ConcurrentHashMap<String, String> contents = new ConcurrentHashMap<String, String>();   // contains coordinator keys
    ConcurrentHashMap<String,String> temp = new ConcurrentHashMap<String,String>();         // contains * Query reply from other AVDs
    ConcurrentHashMap<String,String> replica = new ConcurrentHashMap<String,String>();      // Contains replica Keys

    static final String TAG = SimpleDynamoActivity.class.getSimpleName();
    ContentResolver mContentResolver;
    ArrayList<String> ports = new ArrayList<String>();      // list of all ports
    Object queryLock = new Object();                        // object to synchronize query block
    Object insertLock = new Object();                       // object to synchronize insert block
    StringBuffer finalSend = new StringBuffer();            // StringBuffer for transferring data to the node that recovered
    int count =0;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        SQLiteDatabase db = mDbHelper.getWritableDatabase();
        if (selection.equals("\"@\"")){             // if @ delete all local
            db.delete(mDbHelper.TABLE_NAME,null,null);
            contents= new ConcurrentHashMap<String,String>();
        }
        else if(selection.equals("\"*\"")){         // Unleash the power. delete all

            db.delete(mDbHelper.TABLE_NAME,null,null);
            contents= new ConcurrentHashMap<String,String>();
            BmulticastMessage("DELETE ALL"+"\t"+myPort,ports);

        }

        else{
            if(findLocation(selection).equals(myPort)){    // if key in local
                // return it
                db.delete(mDbHelper.TABLE_NAME, "key=" + "'" + selection + "'", null);
                contents.remove(selection);
            }

            else{
                // delete the replicated copy
                db.delete(mDbHelper.TABLE_NAME, "key=" + "'" + selection + "'", null);
                contents.remove(selection);
                // send to next

                BmulticastMessage("DELETE_SELECTION"+"\t"+ selection,ports);    // if not mine send it to Node that has this key


            }

        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        System.out.println(darkside);
        synchronized (insertLock) {         // synchronize this block with insertLock object
            //System.out.println("STOP WAIT DONE IN INSERT");

            //System.out.println("STOP WAIT DONE IN INSERT");
                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                String key = (String) values.get("key");
                String value = (String) values.get("value");
                System.out.println("TRY INSERT" + "\t" + key);

                try {
                    HashIt = genHash(key);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                // Logic for insert in a ring based topology
                // Node Order based on SHA-1 is 5562,5556,5554,5558,5560
                try {
                    if (HashIt.compareTo(genHash("5562")) <= 0 || HashIt.compareTo(genHash("5560")) > 0) {
                        System.out.println("INSERT IN 5562");
                        if (portStr.equals("5562")) {
                            inserttoSelf(uri, key, value);

                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11112");
                            pee.add("11108");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        } else {
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11112");
                            pee.add("11108");
                            unicastMessage(key + "\t" + value + "\t" + "INSERT", "11124");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));

                        }
                    }
                    if (HashIt.compareTo(genHash("5562")) > 0 && HashIt.compareTo(genHash("5556")) <= 0) {
                        System.out.println("INSERT IN 5556");
                        if (portStr.equals("5556")) {
                            inserttoSelf(uri, key, value);
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11108");
                            pee.add("11116");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        } else {
                            unicastMessage(key + "\t" + value + "\t" + "INSERT", "11112");
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11108");
                            pee.add("11116");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        }

                    }
                    if (HashIt.compareTo(genHash("5556")) > 0 && HashIt.compareTo(genHash("5554")) <= 0) {
                        System.out.println("INSERT IN 5554");
                        if (portStr.equals("5554")) {
                            inserttoSelf(uri, key, value);
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11116");
                            pee.add("11120");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        } else {

                            unicastMessage(key + "\t" + value + "\t" + "INSERT", "11108");
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11116");
                            pee.add("11120");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));

                        }
                    }
                    if (HashIt.compareTo(genHash("5554")) > 0 && HashIt.compareTo(genHash("5558")) <= 0) {
                        System.out.println("INSERT IN 5558");
                        if (portStr.equals("5558")) {
                            inserttoSelf(uri, key, value);
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11120");
                            pee.add("11124");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        } else {

                            unicastMessage(key + "\t" + value + "\t" + "INSERT", "11116");
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11120");
                            pee.add("11124");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        }
                    }
                    if (HashIt.compareTo(genHash("5558")) > 0 && HashIt.compareTo(genHash("5560")) <= 0) {
                        System.out.println("INSERT IN 5560");
                        if (portStr.equals("5560")) {
                            inserttoSelf(uri, key, value);
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11124");
                            pee.add("11112");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        } else {
                            unicastMessage(key + "\t" + value + "\t" + "INSERT", "11120");
                            ArrayList<String> pee = new ArrayList<String>();
                            pee.add("11124");
                            pee.add("11112");
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(0));
                            unicastMessage(key + "\t" + value + "\t" + "REPLICATE", pee.get(1));


                        }
                    }

                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }

            }
        return uri;
    }

    // inser to the local content provider
    private Uri inserttoSelf(Uri uri, String k, String val) {

            vals.put("key", k);
            vals.put("value", val);
            System.out.println("Inside inserttoSelf()");
            SQLiteDatabase db = mDbHelper.getWritableDatabase();
            db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
            contents.put(k, val);
            return uri;


    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        mDbHelper = new FeedReaderDBHelper(getContext());
        // need to create the server and client threads
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(Context.TELEPHONY_SERVICE);
        portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        status= false;
        ports.add("11108");
        ports.add("11116");
        ports.add("11120");
        ports.add("11124");
        ports.add("11112");

        // crete serversocket and servertask to receive messages
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create serverSocket");
        }

        // clienttask sends join message everytime the AVD comes back alive

        mContentResolver = (this.getContext()).getContentResolver();
        String msg = "JOIN"+"\t"+portStr;
        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg);
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        // TODO Auto-generated method stub

        synchronized (queryLock) {      // QueryLock synchronization

            String[] columnNames = {"key", "value"};
            System.out.println("QUERYING" + "\t" + selection);
            m = new MatrixCursor(columnNames);
            SQLiteDatabase db = mDbHelper.getReadableDatabase();
            SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
            builder.setTables(FeedReaderDBHelper.TABLE_NAME);
            Cursor cursor = null;

            if (selection.equals("\"@\"")) {        // if @ return thou content provider

                builder.setTables(FeedReaderDBHelper.TABLE_NAME);
                cursor = builder.query(db, null, null, null, null, null, null);
                return cursor;

            } else if (selection.equals("\"*\""))       // if * return yours + others data

            {
                for (Map.Entry<String, String> entry : contents.entrySet()) {
                    String[] str = new String[2];
                    str[0] = entry.getKey();
                    str[1] = entry.getValue();
                    m.addRow(str);
                }
                for (Map.Entry<String, String> entry : replica.entrySet()) {
                    String[] str = new String[2];
                    str[0] = entry.getKey();
                    str[1] = entry.getValue();
                    m.addRow(str);
                }

                BmulticastMessage("GDUMP" + "\t" + myPort, ports);    // BE MY SLAVE! I COMMAND YOU TO FETCH YOUR VALUES
                try {
                    Thread.sleep(1000);         // you have my permission to sleep
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                for (Map.Entry<String, String> entry : temp.entrySet()) {
                    String[] str = new String[2];
                    str[0] = entry.getKey();
                    str[1] = entry.getValue();
                    m.addRow(str);
                }
                return m;
            } else {
                // Other selection parameters
                System.out.println("Single Key Query" + "\t" + selection);

                    if (findLocation(selection).equals(myPort) || replica.contains(selection)) {   // local key

                        System.out.println("FOUND" + "\t" + selection + "\t" + "IN CURRENT AVD");
                        cursor = builder.query(db, null, "key=" + "'" + selection + "'", null, null, null, null);
                        cursor.moveToFirst();
                        Log.d("SELF CURSOR", DatabaseUtils.dumpCursorToString(cursor));
                        return cursor;
                    } else {
                        System.out.println("NOT FOUND. FORWARDING REQUEST");
                        MatrixCursor a = new MatrixCursor(columnNames);
                        a = queryOther(uri, selection);
                        return a;

                    }
                }
            }



    }
    String globalKey="";
    private MatrixCursor queryOther(Uri uri, String selection) {
        System.out.println("INSIDE QUERY OTHER");
       // synchronized (timepassLock) {
            count++;
            status = false;
            String[] columnNames = {"key", "value"};
            m = new MatrixCursor(columnNames);
            String pea = findLocation(selection);
            looktheshitUp(String.valueOf(Integer.parseInt(pea)/2));
            System.out.println("SUCCC"+Integer.parseInt(succ)*2);
            System.out.println(pea);
            globalKey= selection;
            unicastMessage("QUERY" + "\t" + selection + "\t" + myPort+"\t"+count,pea);
            unicastMessage("DARTHVADER" + "\t" + selection + "\t" + myPort+"\t"+count,String.valueOf(Integer.parseInt(succ)*2));

            while (status != true) {

                }
            System.out.println("STOP WAIT DONE");
            m.moveToFirst();
            Log.d("CURSOR FETCHED", DatabaseUtils.dumpCursorToString(m));
            status=true;

           return m;
        //    }
   }

    private String findLocation(String selection) {
        try {
            if (genHash(selection).compareTo(genHash("5562")) <= 0 || genHash(selection).compareTo(genHash("5560")) > 0) {
                return "11124";
            } else if (genHash(selection).compareTo(genHash("5562")) > 0 && genHash(selection).compareTo(genHash("5556")) <= 0) {
                return "11112";
            } else if (genHash(selection).compareTo(genHash("5556")) > 0 && genHash(selection).compareTo(genHash("5554")) <= 0) {
                return "11108";
            } else if (genHash(selection).compareTo(genHash("5554")) > 0 && genHash(selection).compareTo(genHash("5558")) <= 0) {
                return "11116";
            } else {
                return "11120";
            }
        }catch(NoSuchAlgorithmException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
	public int update(Uri uri, ContentValues values, String selection,
			String[] selectionArgs) {
		// TODO Auto-generated method stub
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


            try {

                while (true) {
                    Socket listener = serverSocket.accept();
                    BufferedReader in = new BufferedReader(new InputStreamReader(listener.getInputStream()));
                    String line = null;
                    while ((line = in.readLine()) != null) {
                        line = line.trim();
                        if(line.contains("INSERT")){        // insert event

                            synchronized (insertLock) {
                                String token[] = line.split("\t");
                                System.out.println("INSERT" + "\t" + token[0]);

                                vals.put("key", token[0]);
                                vals.put("value", token[1]);
                                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                                db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
                                contents.put(token[0], token[1]);
                            }

                        }
                        if(line.contains("REPLICATE")){         // replication event

                            synchronized (insertLock) {
                                String token[] = line.split("\t");
                                vals.put("key", token[0]);
                                vals.put("value", token[1]);
                                System.out.println("REPLICATING \t" + token[0] + "in" + "\t" + myPort);
                                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                                replica.put(token[0],token[1]);
                                db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
                            }
                        }
                        if(line.contains("QUERY")){             // query event
                            String token[] = line.split("\t");
                            System.out.println("INSIDE QUERY"+"\t"+line);

                            if (findLocation(token[1]).equals(myPort)) {
                                String countCheck = token[3];
                                SQLiteDatabase db = mDbHelper.getReadableDatabase();
                                SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
                                builder.setTables(FeedReaderDBHelper.TABLE_NAME);
                                Cursor cursor = null;
                                cursor = builder.query(db, null, "key=" + "'" + token[1] + "'", null, null, null, null);
                                if (cursor.moveToFirst()){
                                    do{
                                        String Key = cursor.getString(cursor.getColumnIndex("key"));
                                        String Value= cursor.getString(cursor.getColumnIndex("value"));
                                        // do what ever you want here
                                        unicastMessage(Key + "\t" + Value + "\t" + "FINAL"+"\t"+countCheck, token[2]); // send to sender
                                        System.out.println("UNICASTING"+"\t"+token[1]+"\t"+token[2]);
                                    }while(cursor.moveToNext());
                                }
                                cursor.close();
                              }

                        }
                        if(line.contains("DARTHVADER")){            // *DARTHVADER MUSIC*
                                                                    // little hack. at any given time only 1 node can fail
                            System.out.println(line);               // so query the coordinator and one of its successor. and return one cursor
                            String token[]= line.split("\t");
                            String countCheck = token[3];
                            SQLiteDatabase db = mDbHelper.getReadableDatabase();
                            SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
                            builder.setTables(FeedReaderDBHelper.TABLE_NAME);
                            Cursor cursor = null;
                            cursor = builder.query(db, null, "key=" + "'" + token[1] + "'", null, null, null, null);
                            if (cursor.moveToFirst()){
                                do{
                                    String Key = cursor.getString(cursor.getColumnIndex("key"));
                                    String Value= cursor.getString(cursor.getColumnIndex("value"));
                                    // do what ever you want here
                                    unicastMessage(Key + "\t" + Value + "\t" + "ALT"+"\t"+countCheck, token[2]); // send to sender
                                    System.out.println("UNICASTING"+"\t"+token[1]+"\t"+token[2]);
                                }while(cursor.moveToNext());
                            }
                            cursor.close();
                           // Log.d("ALT CURSOR", DatabaseUtils.dumpCursorToString(cursor));

                        }
                        if (line.contains("FINAL")) {           // Query reply from coordinator
                            System.out.println("FETCHED QUERY FROM APPROPRIATE AVD");
                            if(!globalKey.equals("")) {
                                if (globalKey.equals(line.split("\t")[0]) ) {
                                    String token[] = line.split("\t");
                                    System.out.println(line);
                                    String[] columnNames = {"key", "value"};
                                    m = new MatrixCursor(columnNames);
                                    String[] str = new String[2];
                                    str[0] = token[0];
                                    str[1] = token[1];
                                    m.addRow(str);
                                    //m.addRow(str);
                                    //m.moveToFirst();
                                    Log.d("CURSOR FINAL", DatabaseUtils.dumpCursorToString(m));
                                    globalKey = "";
                                    status = true;
                                    System.out.println(status);

                                }
                            }

                        }
                        if(line.contains("ALT")){       // query reply from DARTHVADER aka successor
                            if(!globalKey.equals("")) {
                                if (globalKey.equals(line.split("\t")[0]) ) {
                                    System.out.println("FETCHED QUERY FROM APPROPRIATE AVD_ALT");
                                    String token[] = line.split("\t");
                                    System.out.println(line);
                                    String[] columnNames = {"key", "value"};
                                    m = new MatrixCursor(columnNames);
                                    String[] str = new String[2];
                                    str[0] = token[0];
                                    str[1] = token[1];
                                    m.addRow(str);
                                    //m.addRow(str);
                                    //m.moveToFirst();
                                    Log.d("CURSOR FINAL_ALT", DatabaseUtils.dumpCursorToString(m));
                                    globalKey = "";
                                    status = true;
                                    System.out.println(status);
                                    
                                }
                            }
                        }
                        if(line.contains("DELETE ALL")){            // Thou delete everything!
                            SQLiteDatabase db = mDbHelper.getWritableDatabase();
                            db.delete(mDbHelper.TABLE_NAME, null, null);
                            contents= new ConcurrentHashMap<String,String>();
                        }
                        if(line.contains("DELETE_SELECTION")){      // thou delete one!
                            if(findLocation(line.split("\t")[1]).equals(myPort)){
                                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                                db.delete(mDbHelper.TABLE_NAME, "key=" + "'" + line.split("\t")[1] + "'", null);
                                contents.remove(line.split("\t")[1]);
                            }
                        }
                        if(line.contains("GDUMP")){             // Dump everything!
                            StringBuffer br = new StringBuffer();
                            for (Map.Entry<String, String> entry : contents.entrySet()) {
                                br.append(entry.getKey() + "\t" + entry.getValue()+"|");


                            }
                            for (Map.Entry<String, String> entry : replica.entrySet()) {
                                br.append(entry.getKey() + "\t" + entry.getValue()+"|");


                            }
                            try {
                                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                        Integer.parseInt(line.split("\t")[1]));
                                String msgToSend = br.toString() + "=" + "COPYBACK";
                                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                                msgToSend = msgToSend.trim();
                                out.print(msgToSend);

                                out.flush();
                                socket.close();
                            } catch (UnknownHostException e) {
                                e.printStackTrace();
                            }
                            br.delete(0, br.length());
                        }
                        if(line.contains("COPYBACK")){          // collect * query responses
                            String cleanup[] = line.split("=");
                            String token[] = cleanup[0].split("\\|");
                            for(int i=0;i<=token.length-1;i++){
                                String keyVal[] = token[i].split("\t");
                                temp.put(keyVal[0],keyVal[1]);

                            }

                        }
                        if(line.contains("JOIN")) {             // failure handling
                            System.out.println(line);
                            String
                                    token[] = line.split("\t");
                            SQLiteDatabase db = mDbHelper.getReadableDatabase();
                            SQLiteQueryBuilder builder = new SQLiteQueryBuilder();
                            builder.setTables(FeedReaderDBHelper.TABLE_NAME);
                            Cursor cursor = null;
                            cursor = builder.query(db, null, null, null, null, null, null);
                            if (cursor.getCount() > 0) {
                                new recoveryTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, line);
                            }
                        }
                        if(line.contains("STORE")) {
                            synchronized (insertLock) {

                                    darkside = 1;
                                    String cleanup[] = line.split("=");
                                    busywait = true;
                                    if (!cleanup[0].isEmpty()) {
                                        if (cleanup[1].contains("STORE_REPLICA")) {
                                            String token[] = cleanup[0].split("\\|");
                                            for (int i = 0; i <= token.length - 1; i++) {
                                                String keyVal[] = token[i].split("\t");
                                                vals.put("key", keyVal[0]);
                                                vals.put("value", keyVal[1]);
                                                System.out.println(keyVal[0] + "\t" + keyVal[1]);
                                                SQLiteDatabase db = mDbHelper.getWritableDatabase();
                                                replica.put(keyVal[0], keyVal[1]);
                                                db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);

                                            }

                                        }
                                        if (cleanup[1].contains("STORE_COORD")) {
                                            String token[] = cleanup[0].split("\\|");
                                            System.out.println("LINE" + "\t" + line);
                                            System.out.println("cleanup" + "\t" + cleanup[0]);
                                            for (int i = 0; i <= token.length - 1; i++) {
                                                String keyVal[] = token[i].split("\t");
                                                if (findLocation(keyVal[0]).equals(myPort)) {
                                                    vals.put("key", keyVal[0]);
                                                    vals.put("value", keyVal[1]);
                                                    System.out.println(keyVal[0] + "\t" + keyVal[1]);
                                                    SQLiteDatabase db = mDbHelper.getWritableDatabase();
                                                    contents.put(keyVal[0], keyVal[1]);
                                                    db.insertWithOnConflict(mDbHelper.TABLE_NAME, null, vals, SQLiteDatabase.CONFLICT_REPLACE);
                                                }


                                            }
                                        }
                                        darkside = 0;
                                        busywait = false;

                                    }
                                }


                            }
                       // }

                    }
                    listener.close();
                }
            }catch (IOException e) {
                e.printStackTrace();
            }

            return null;
        }
    }

    private void looktheshitUp(String token) {
        if(token.equals("5562")){
            pred= "5560";
            pred1 = "5558";
            succ = "5556";
            succ1 = "5554";
        }
        if(token.equals("5556")){
            pred = "5562";
            pred1= "5560";
            succ = "5554";
            succ1 = "5558";

        }
        if(token.equals("5554")){
            pred = "5556";
            pred1= "5562";
            succ = "5558";
            succ1 = "5560";

        }
        if(token.equals("5558")){
            pred = "5554";
            pred1= "5556";
            succ = "5560";
            succ1 = "5562";

        }
        if(token.equals("5560")){
            pred = "5558";
            pred1= "5554";
            succ = "5562";
            succ1 = "5556";

        }
    }

    private  class recoveryTask extends AsyncTask <String,Void,Void> {
        @Override
        protected Void doInBackground(String... msgs) {
                String recoveredNode  = msgs[0].split("\t")[1];
                  looktheshitUp(recoveredNode);
                    if(portStr.equals(pred)|| portStr.equals(pred1)){

                        for (Map.Entry<String, String> entry : contents.entrySet()) {
                            finalSend.append(entry.getKey() + "\t" + entry.getValue()+"|");

                        }
                        try {
                            String port = String.valueOf(Integer.parseInt(recoveredNode)*2);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(port));
                            String msgToSend = finalSend.toString()+"="+"STORE_REPLICA";
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            msgToSend = msgToSend.trim();
                            out.print(msgToSend);
                            out.flush();
                            socket.close();
                            finalSend.delete(0,finalSend.length());

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                        }


                    }

                    if(portStr.equals(succ1) || portStr.equals(succ)){
                        for (Map.Entry<String, String> entry : replica.entrySet()) {
                            //String Key = entry.getKey();    // check if key is the coord of recovered
                            finalSend.append(entry.getKey() + "\t" + entry.getValue()+"|");
                            }

                        }
                        try {
                            String port = String.valueOf(Integer.parseInt(recoveredNode)*2);
                            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                    Integer.parseInt(port));
                            String msgToSend = finalSend.toString()+"="+"STORE_COORD";
                            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                            msgToSend = msgToSend.trim();
                            out.print(msgToSend);
                            out.flush();
                            socket.close();
                            finalSend.delete(0,finalSend.length());

                        } catch (UnknownHostException e) {
                            Log.e(TAG, "ClientTask UnknownHostException");
                        } catch (IOException e) {
                            Log.e(TAG, "ClientTask socket IOException");
                 }

            return null;
        }

    }


    private Uri setupURI(String content, String s) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(s);
        uriBuilder.scheme(content);
        return uriBuilder.build();
    }


    private class ClientTask extends AsyncTask<String, Void, Void> {
        @Override
        protected Void doInBackground(String... msgs) {
            //build a string array of ports to send to
            String[] remotePort = {"11108", "11116", "11120", "11124", "11112"};
            for (int i = 0; i < 5; i++) {
                if (!remotePort[i].equals(myPort)) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                                Integer.parseInt(remotePort[i]));
                        String msgToSend = msgs[0];
                        PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                        msgToSend = msgToSend.trim();
                        out.print(msgToSend);
                        out.flush();
                        socket.close();
                    } catch (UnknownHostException e) {
                        Log.e(TAG, "ClientTask UnknownHostException");
                    } catch (IOException e) {
                        Log.e(TAG, "ClientTask socket IOException");
                    }
                }
            }

            return null;

        }
    }

    public void BmulticastMessage(String msg, ArrayList<String> pt){

        for(int i = 0; i <=pt.size()-1; i++) {
            try {
                // send happens here
                if(myPort!=(pt.get(i))){
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(pt.get(i)));
                    String msgToSend = msg;
                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    msgToSend = msgToSend.trim();
                    out.print(msgToSend);
                    out.flush();
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
        }
    }

    public void unicastMessage(String msg, String pt){

        try {
            // send happens here
            Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                    Integer.parseInt(pt));
            String msgToSend = msg;
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            msgToSend = msgToSend.trim();
            out.print(msgToSend);
            out.flush();
            socket.close();

        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
        } catch (IOException e) {
            Log.e(TAG, "ClientTask socket IOException");
        }

    }
}

